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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleCoordinatorContext<K> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleCoordinatorContext.class);

    private final ExecutorService coordinatorExecutor;
    private final OperatorCoordinator.Context operatorCoordinatorContext;
    private final OperatorCoordinator.SubtaskGateway[] subtaskGateways;
    private final DataStatisticsCoordinatorProvider.CoordinatorExecutorThreadFactory
            coordinatorThreadFactory;

    ShuffleCoordinatorContext(
            ExecutorService coordinatorExecutor,
            DataStatisticsCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory,
            OperatorCoordinator.Context operatorCoordinatorContext) {
        this.coordinatorExecutor = coordinatorExecutor;
        this.coordinatorThreadFactory = coordinatorThreadFactory;
        this.operatorCoordinatorContext = operatorCoordinatorContext;
        this.subtaskGateways =
                new OperatorCoordinator.SubtaskGateway
                        [operatorCoordinatorContext.currentParallelism()];
    }

    @Override
    public void close() throws Exception {
        coordinatorExecutor.shutdown();
        coordinatorExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    void sendDataDistributionWeightToSubtasks(DataDistributionWeight<K> dataDistributionWeight) {
        callInCoordinatorThread(
                () -> {
                    DataDistributionWeightEvent<K> dataDistributionWeightEvent =
                            new DataDistributionWeightEvent<K>(dataDistributionWeight);
                    for (final OperatorCoordinator.SubtaskGateway subtaskGateway : subtaskGateways) {
                        subtaskGateway.sendEvent(dataDistributionWeightEvent);
                    }
                    return null;
                },
                String.format("Failed to send data distribution weight %s", dataDistributionWeight));
    }

    int currentParallelism() {
        return operatorCoordinatorContext.currentParallelism();
    }

    // --------- Package private additional methods for the ShuffleCoordinator ------------

    void runInCoordinatorThread(Runnable runnable) {
        coordinatorExecutor.execute(runnable);
    }

    void subtaskReady(OperatorCoordinator.SubtaskGateway gateway) {
        final int subtask = gateway.getSubtask();
        if (subtaskGateways[subtask] == null) {
            subtaskGateways[subtask] = gateway;
        } else {
            throw new IllegalStateException("Already have a subtask gateway for " + subtask);
        }
    }

    void subtaskNotReady(int subtaskIndex) {
        subtaskGateways[subtaskIndex] = null;
    }

    void failJob(Throwable cause) {
        operatorCoordinatorContext.failJob(cause);
    }

    /**
     * A helper method that delegates the callable to the coordinator thread if the current thread
     * is not the coordinator thread, otherwise call the callable right away.
     *
     * @param callable the callable to delegate.
     */
    private <V> V callInCoordinatorThread(Callable<V> callable, String errorMessage) {
        // Ensure the task is done by the coordinator executor.
        if (!coordinatorThreadFactory.isCurrentThreadCoordinatorThread()) {
            try {
                final Callable<V> guardedCallable =
                        () -> {
                            try {
                                return callable.call();
                            } catch (Throwable t) {
                                LOG.error("Uncaught Exception in Shuffle Coordinator Executor", t);
                                ExceptionUtils.rethrowException(t);
                                return null;
                            }
                        };

                return coordinatorExecutor.submit(guardedCallable).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new FlinkRuntimeException(errorMessage, e);
            }
        }

        try {
            return callable.call();
        } catch (Throwable t) {
            LOG.error("Uncaught Exception in shuffle coordinator executor", t);
            throw new FlinkRuntimeException(errorMessage, t);
        }
    }
}
