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

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DistinctLimitSpilledOperator
        implements Operator
{
    private static final int MAX_BYTES_PER_CHANNEL = 256 * 1024 * 1024;

    private ExternalPagesSortChannelMerger externalPagesSortChannelMerger;

    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;

    private long remainingLimit;
    private boolean writeFinish;
    private boolean readFinish;

    public static class DistinctLimitSpilledOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Integer> distinctChannels;
        private final List<Type> sourceTypes;
        private final long limit;
        private final Optional<Integer> hashChannel;
        private boolean closed;
        private final JoinCompiler joinCompiler;
        private final PagesIndex.Factory pagesIndexFactory;
        private final SingleStreamSpillerFactory singleStreamSpillerFactory;
        private final boolean enableSpill;

        public DistinctLimitSpilledOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> distinctChannels,
                long limit,
                Optional<Integer> hashChannel,
                JoinCompiler joinCompiler,
                PagesIndex.Factory pagesIndexFactory,
                SingleStreamSpillerFactory singleStreamSpillerFactory,
                boolean enableSpill)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.distinctChannels = requireNonNull(distinctChannels, "distinctChannels is null");

            checkArgument(limit >= 0, "limit must be at least zero");
            this.limit = limit;
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "PagesIndex.Factory is null");
            this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "SingleStreamSpillFactory");
            this.enableSpill = enableSpill;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, DistinctLimitOperator.class.getSimpleName());
            List<Type> distinctTypes = distinctChannels.stream()
                    .map(sourceTypes::get)
                    .collect(toImmutableList());
            return new DistinctLimitSpilledOperator(operatorContext, distinctChannels, distinctTypes, limit, hashChannel, joinCompiler, pagesIndexFactory, singleStreamSpillerFactory);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new DistinctLimitSpilledOperatorFactory(operatorId, planNodeId, sourceTypes, distinctChannels, limit, hashChannel, joinCompiler, pagesIndexFactory, singleStreamSpillerFactory, enableSpill);
        }
    }

    public DistinctLimitSpilledOperator(OperatorContext operatorContext,
            List<Integer> distinctChannels,
            List<Type> distinctTypes,
            long limit,
            Optional<Integer> hashChannel,
            JoinCompiler joinCompiler,
            PagesIndex.Factory factory,
            SingleStreamSpillerFactory singleStreamSpillerFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        requireNonNull(distinctChannels, "distinctChannels is null");
        checkArgument(limit >= 0, "limit must be at least zero");
        requireNonNull(hashChannel, "hashChannel is null");

        List<Integer> sortChannels = new ArrayList<>();
        List<SortOrder> sortOrders = new ArrayList<>();
        for (int i = 0; i < distinctChannels.size(); i++) {
            sortChannels.add(i);
            sortOrders.add(SortOrder.ASC_NULLS_FIRST);
        }

        this.externalPagesSortChannelMerger = new ExternalPagesSortChannelMerger(
                distinctTypes,
                sortChannels,
                sortOrders,
                factory,
                singleStreamSpillerFactory,
                this.operatorContext,
                MAX_BYTES_PER_CHANNEL);
        remainingLimit = limit;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> spillInProgress = this.externalPagesSortChannelMerger.spillInProgress();
        return spillInProgress == null ? NOT_BLOCKED : spillInProgress;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (writeFinish == false) {
            this.externalPagesSortChannelMerger.finalizePages();
        }
        writeFinish = true;
    }

    @Override
    public boolean isFinished()
    {
        return writeFinish && readFinish;
    }

    @Override
    public boolean needsInput()
    {
        if (!requestMemory()) {
            return false;
        }

        return writeFinish == false;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput());
        this.externalPagesSortChannelMerger.tryClearSpillInProgressFuture();
        this.externalPagesSortChannelMerger.addPage(page);
    }

    @Override
    public Page getOutput()
    {
        ListenableFuture<?> future = this.externalPagesSortChannelMerger.spillInProgress();
        if (future != null && !future.isDone()) {
            return null;
        }

        this.externalPagesSortChannelMerger.tryClearSpillInProgressFuture();

        if (!writeFinish) {
            return null;
        }

        if (!this.externalPagesSortChannelMerger.isReadReady()) {
            return null;
        }

        if (!requestMemory()) {
            return null;
        }

        if (!this.externalPagesSortChannelMerger.hashNextRow()) {
            this.readFinish = true;
            return null;
        }
        Page page = this.externalPagesSortChannelMerger.getPage((int) this.remainingLimit);
        remainingLimit = remainingLimit - page.getPositionCount();

        if (remainingLimit <= 0) {
            this.readFinish = true;
        }

        return page;
    }

    private boolean requestMemory()
    {
        long bytes = this.externalPagesSortChannelMerger.getEstimatedSize()
                + PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES * 2;
        this.localUserMemoryContext.setBytes(bytes);
        return operatorContext.isWaitingForMemory().isDone();
    }

    @Override
    public void close()
            throws Exception
    {
        this.externalPagesSortChannelMerger.close();
    }

    @VisibleForTesting
    public int getCapacity()
    {
        return 0;
    }
}
