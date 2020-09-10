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
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.SingleStreamSpiller;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class ExternalPagesSortChannel
        implements Closeable
{
    private final List<Type> types;

    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;

    private final PagesIndex.Factory pagesIndexFactory;
    private PagesIndex pagesIndex;

    private final SingleStreamSpillerFactory singleStreamSpillerFactory;
    private final OperatorContext operatorContext;

    private Iterator<Page> outPages;
    private int currentOutputPageIndex;
    private Page currentOutputPage;
    private SingleStreamSpiller spiller;

    private ListenableFuture<?> spilling;

    private long memorySizeInBytes;

    public ExternalPagesSortChannel(
            List<Type> types,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            PagesIndex.Factory pagesIndexFactory,
            SingleStreamSpillerFactory singleStreamSpillerFactory,
            OperatorContext operatorContext)
    {
        this.operatorContext = operatorContext;
        this.singleStreamSpillerFactory = singleStreamSpillerFactory;
        this.types = ImmutableList.copyOf(types);
        this.sortChannels = ImmutableList.copyOf(sortChannels);
        this.sortOrders = ImmutableList.copyOf(sortOrders);
        this.pagesIndexFactory = pagesIndexFactory;
        this.pagesIndex = this.pagesIndexFactory.newPagesIndex(types, 16);
    }

    public boolean hasNextRow()
    {
        if (currentOutputPage == null) {
            return false;
        }
        return this.currentOutputPageIndex < this.currentOutputPage.getPositionCount();
    }

    public void getSpilledPages()
    {
        Iterator<Page> outPages = this.spiller.getSpilledPages();
        this.outPages = outPages;
        if (outPages.hasNext()) {
            this.currentOutputPage = outPages.next();
        }
    }

    public void getNextRow(PageBuilder pageBuilderWriteTo, PageBuilder pageCompareLastRow, int lastRowPosition)
    {
        if (pageCompareLastRow != null) {
            boolean lastRowEqNextRow = true;
            for (int i = 0; i < types.size(); i++) {
                Type type = types.get(i);
                boolean equal = TypeUtils.positionEqualsPosition(type,
                        pageCompareLastRow.getBlockBuilder(i),
                        lastRowPosition,
                        this.currentOutputPage.getBlock(i),
                        this.currentOutputPageIndex);
                if (!equal) {
                    lastRowEqNextRow = false;
                    break;
                }
            }
            if (lastRowEqNextRow) {
                moveToNextRow();
                return;
            }
        }

        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            type.appendTo(this.currentOutputPage.getBlock(i), this.currentOutputPageIndex, pageBuilderWriteTo.getBlockBuilder(i));
        }
        pageBuilderWriteTo.declarePosition();
        moveToNextRow();
    }

    private void moveToNextRow()
    {
        this.currentOutputPageIndex++;
        if (this.currentOutputPageIndex >= this.currentOutputPage.getPositionCount()) {
            this.currentOutputPageIndex = 0;
            this.currentOutputPage = null;
            if (outPages.hasNext()) {
                this.currentOutputPage = outPages.next();
            }
        }
    }

    public int getCurrentOutputPageIndex()
    {
        return currentOutputPageIndex;
    }

    public Page getCurrentOutputPage()
    {
        return currentOutputPage;
    }

    public long getEstimatedSize()
    {
        return this.memorySizeInBytes;
    }

    public void appendPage(Page page)
    {
        this.pagesIndex.addPage(page);
        this.memorySizeInBytes = (this.pagesIndex.getEstimatedSize().toBytes()) * 2;
    }

    public void spillPages()
    {
        LinkedList<Page> outputPages = createOutputPages();

        Optional<SingleStreamSpiller> spiller = Optional.of(singleStreamSpillerFactory.create(
                types,
                operatorContext.getSpillContext().newLocalSpillContext(),
                operatorContext.newLocalSystemMemoryContext(ExternalPagesSortChannel.class.getSimpleName())));
        this.spiller = spiller.get();
        ListenableFuture<?> future = this.spiller.spill(outputPages.iterator());
        this.spilling = future;
    }

    public ListenableFuture<?> getSpillingFuture()
    {
        return spilling;
    }

    public void spillFinish()
    {
        this.spilling = null;
        releaseAndEstimateMemory();
    }

    private void releaseAndEstimateMemory()
    {
        this.pagesIndex.clear();
        this.pagesIndex = null;
        this.memorySizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES * 2;
    }

    private LinkedList<Page> createOutputPages()
    {
        pagesIndex.sort(this.sortChannels, this.sortOrders);

        LinkedList<Page> pages = new LinkedList<>();
        PageBuilder pageBuilder = null;
        for (int i = 0; i < this.pagesIndex.getPositionCount(); i++) {
            if (pageBuilder == null) {
                pageBuilder = new PageBuilder(this.types);
                pageBuilder.reset();
            }

            for (int t = 0; t < this.types.size(); t++) {
                this.pagesIndex.appendTo(t, i, pageBuilder.getBlockBuilder(t));
            }
            pageBuilder.declarePosition();

            if (pageBuilder.isFull()) {
                pages.addLast(pageBuilder.build());
                pageBuilder.reset();
            }
        }

        if (pageBuilder.getPositionCount() > 0) {
            pages.add(pageBuilder.build());
        }

        return pages;
    }

    @Override
    public void close()
            throws IOException
    {
        if (this.spiller != null) {
            this.spiller.close();
        }
    }
}
