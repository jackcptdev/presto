/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import static java.util.Objects.requireNonNull;

public class ExternalPagesSortChannelMerger
        implements Closeable
{
    private final List<Type> types;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;
    private final PagesIndex.Factory pagesIndexFactory;
    private final List<ExternalPagesSortChannel> externalPagesChannels;
    private final PriorityQueue<ExternalPagesSortChannel> mergeQueue;
    private final long maxSizeInBytesPerChannel;
    private final List<Page> currentMemoryPages;
    private final SingleStreamSpillerFactory singleStreamSpillerFactory;
    private final OperatorContext operatorContext;

    private ExternalPagesSortChannel spillingChannel;
    private PageBuilder lastRowPageBuilderForCompare;
    private boolean writeFinish;
    private boolean readReady;
    private long memorySizeInBytes;

    public ExternalPagesSortChannelMerger(List<Type> types,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            PagesIndex.Factory pagesIndexFactory,
            SingleStreamSpillerFactory singleStreamSpillerFactory,
            OperatorContext operatorContext,
            int maxSizeInBytesPerChannel)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "");
        this.types = ImmutableList.copyOf(types);
        this.sortChannels = ImmutableList.copyOf(sortChannels);
        this.sortOrders = ImmutableList.copyOf(sortOrders);
        this.pagesIndexFactory = pagesIndexFactory;
        this.externalPagesChannels = new ArrayList<>(16);
        this.currentMemoryPages = new ArrayList<>(16);
        this.maxSizeInBytesPerChannel = maxSizeInBytesPerChannel;
        this.mergeQueue = new PriorityQueue<ExternalPagesSortChannel>(new Comparator<ExternalPagesSortChannel>()
        {
            @Override
            public int compare(ExternalPagesSortChannel leftChannel, ExternalPagesSortChannel rightChannel)
            {
                Page left = leftChannel.getCurrentOutputPage();
                int leftIndex = leftChannel.getCurrentOutputPageIndex();
                Page right = rightChannel.getCurrentOutputPage();
                int rightIndex = rightChannel.getCurrentOutputPageIndex();
                for (int i = 0; i < sortChannels.size(); i++) {
                    int sortChannel = sortChannels.get(i);
                    Block leftBlock = left.getBlock(sortChannel);
                    Block rightBlock = right.getBlock(sortChannel);
                    SortOrder sortOrder = sortOrders.get(i);
                    Type type = types.get(sortChannel);
                    int compare = sortOrder.compareBlockValue(type, leftBlock, leftIndex, rightBlock, rightIndex);
                    if (compare != 0) {
                        return compare;
                    }
                }
                return 0;
            }
        });
    }

    public ListenableFuture<?> spillInProgress()
    {
        return this.spillingChannel == null ? null : this.spillingChannel.getSpillingFuture();
    }

    public void tryClearSpillInProgressFuture()
    {
        if (this.spillingChannel != null && this.spillingChannel.getSpillingFuture().isDone()) {
            this.spillingChannel.spillFinish();
            this.spillingChannel = null;

            if (this.writeFinish) {
                prepareRead();
                this.readReady = true;
            }
        }
    }

    public boolean isReadReady()
    {
        return readReady;
    }

    public Page getPage(int maxRowCount)
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        pageBuilder.reset();

        while (pageBuilder.getPositionCount() < maxRowCount && hashNextRow()) {
            if (pageBuilder.getPositionCount() == 0) {
                getNextRow(pageBuilder, lastRowPageBuilderForCompare, 0);
            }
            else {
                getNextRow(pageBuilder, pageBuilder, pageBuilder.getPositionCount() - 1);
            }
            if (pageBuilder.isFull()) {
                break;
            }
        }

        Page page = pageBuilder.build();
        if (pageBuilder.getPositionCount() > 0) {
            lastRowPageBuilderForCompare = new PageBuilder(1, types);
            lastRowPageBuilderForCompare.reset();
            for (int i = 0; i < types.size(); i++) {
                Type type = types.get(i);
                type.appendTo(page.getBlock(i), page.getPositionCount() - 1, lastRowPageBuilderForCompare.getBlockBuilder(i));
            }
        }

        return page;
    }

    public void getNextRow(PageBuilder pageBuilder, PageBuilder lastRow, int lastRowPosition)
    {
        ExternalPagesSortChannel channel = this.mergeQueue.poll();
        channel.getNextRow(pageBuilder, lastRow, lastRowPosition);
        if (channel.hasNextRow()) {
            this.mergeQueue.offer(channel);
        }
    }

    public boolean hashNextRow()
    {
        return mergeQueue.size() > 0;
    }

    public void addPage(Page page)
    {
        this.currentMemoryPages.add(page);
        this.memorySizeInBytes += (page.getSizeInBytes() * 2);

        if (needFlush()) {
            flushPages();
        }
    }

    public void finalizePages()
    {
        if (this.currentMemoryPages.size() > 0) {
            flushPages();
        }
        this.writeFinish = true;
    }

    private void prepareRead()
    {
        if (writeFinish == false) {
            return;
        }
        this.readReady = true;

        for (ExternalPagesSortChannel externalPagesChannel : this.externalPagesChannels) {
            externalPagesChannel.getSpilledPages();
            if (externalPagesChannel.hasNextRow()) {
                this.mergeQueue.offer(externalPagesChannel);
            }
        }
    }

    public long getEstimatedSize()
    {
        long size = 0;
        for (ExternalPagesSortChannel channel : this.externalPagesChannels) {
            size += channel.getEstimatedSize();
        }
        size += memorySizeInBytes;
        return size;
    }

    private void flushPages()
    {
        ExternalPagesSortChannel externalPagesChannel = new ExternalPagesSortChannel(
                this.types,
                this.sortChannels,
                this.sortOrders,
                this.pagesIndexFactory,
                this.singleStreamSpillerFactory, this.operatorContext);
        for (Page page : this.currentMemoryPages) {
            externalPagesChannel.appendPage(page);
        }
        this.spillingChannel = externalPagesChannel;
        this.externalPagesChannels.add(externalPagesChannel);
        externalPagesChannel.spillPages();
        this.currentMemoryPages.clear();
        this.memorySizeInBytes = 0;
    }

    private boolean needFlush()
    {
        return this.memorySizeInBytes >= maxSizeInBytesPerChannel;
    }

    @Override
    public void close()
            throws IOException
    {
        for (ExternalPagesSortChannel externalPagesSortChannel : this.externalPagesChannels) {
            externalPagesSortChannel.close();
        }
    }
}
