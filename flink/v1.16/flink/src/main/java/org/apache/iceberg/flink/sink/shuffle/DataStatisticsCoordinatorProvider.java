/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink.shuffle;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nullable;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.iceberg.flink.sink.shuffle.statistics.DataStatisticsFactory;
import org.jetbrains.annotations.NotNull;

public class DataStatisticsCoordinatorProvider<K extends Serializable> extends RecreateOnResetOperatorCoordinator.Provider {

    private final String operatorName;
    private final DataStatisticsFactory<K> dataStatisticsFactory;
    private final int maxAggregatedDataDistributionHistoryToKeep;

    public DataStatisticsCoordinatorProvider(String operatorName,
                                             OperatorID operatorID,
                                             DataStatisticsFactory<K> dataStatisticsFactory,
                                             int maxAggregatedDataDistributionHistoryToKeep) {
        super(operatorID);
        this.operatorName = operatorName;
        this.dataStatisticsFactory = dataStatisticsFactory;
        this.maxAggregatedDataDistributionHistoryToKeep = maxAggregatedDataDistributionHistoryToKeep;
    }

    @Override
    public OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
        DataStatisticsCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory =
                new DataStatisticsCoordinatorProvider.CoordinatorExecutorThreadFactory(
                        "ShuffleCoordinator-" + operatorName, context.getUserCodeClassloader());
        ExecutorService coordinatorExecutor =
                Executors.newSingleThreadExecutor(coordinatorThreadFactory);
        ShuffleCoordinatorContext<K> shuffleCoordinatorContext =
                new ShuffleCoordinatorContext<>(coordinatorExecutor, coordinatorThreadFactory, context);

        return new DataStatisticsCoordinator<>(operatorName, coordinatorExecutor, shuffleCoordinatorContext, dataStatisticsFactory,
                maxAggregatedDataDistributionHistoryToKeep);
    }

    static class CoordinatorExecutorThreadFactory
            implements ThreadFactory, Thread.UncaughtExceptionHandler {

        private final String coordinatorThreadName;
        private final ClassLoader cl;
        private final Thread.UncaughtExceptionHandler errorHandler;

        @Nullable
        private Thread thread;

        CoordinatorExecutorThreadFactory(
                final String coordinatorThreadName, final ClassLoader contextClassLoader) {
            this(coordinatorThreadName, contextClassLoader, FatalExitExceptionHandler.INSTANCE);
        }

        @VisibleForTesting
        CoordinatorExecutorThreadFactory(
                final String coordinatorThreadName,
                final ClassLoader contextClassLoader,
                final Thread.UncaughtExceptionHandler errorHandler) {
            this.coordinatorThreadName = coordinatorThreadName;
            this.cl = contextClassLoader;
            this.errorHandler = errorHandler;
        }

        @Override
        public synchronized Thread newThread(@NotNull Runnable r) {
            thread = new Thread(r, coordinatorThreadName);
            thread.setContextClassLoader(cl);
            thread.setUncaughtExceptionHandler(this);
            return thread;
        }

        @Override
        public synchronized void uncaughtException(Thread t, Throwable e) {
            errorHandler.uncaughtException(t, e);
        }

        boolean isCurrentThreadCoordinatorThread() {
            return Thread.currentThread() == thread;
        }
    }
}
