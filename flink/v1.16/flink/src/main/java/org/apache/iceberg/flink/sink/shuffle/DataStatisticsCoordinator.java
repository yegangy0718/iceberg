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

import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;
import org.apache.iceberg.flink.sink.shuffle.statistics.DataStatisticsFactory;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataStatisticsOperator collects traffic distribution statistics. A custom partitioner shall be
 * attached to the DataStatisticsOperator output. The custom partitioner leverages the statistics to
 * shuffle record to improve data clustering while maintaining relative balanced traffic
 * distribution to downstream subtasks.
 */
class DataStatisticsCoordinator<K> implements OperatorCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(DataStatisticsCoordinator.class);

  private final String operatorName;
  // A single-thread executor to handle all the changes to the coordinator
  private final ExecutorService coordinatorExecutor;
  private final ShuffleCoordinatorContext<K> context;
  // The max size for aggregateDataStatisticsMap which is the max aggregateDataStatistics history
  // to keep based on checkpoint
  private final int maxAggregateDataDistributionHistoryToKeep;
  // key is the checkpoint id, value is the DataDistributionWeight at the corresponding checkpoint
  private final SortedMap<Long, DataDistributionWeight<K>> aggregateDataStatisticsMap = new TreeMap<>();
  private volatile DataDistributionWeight<K> dataDistributionWeight;
  // A flag marking whether the coordinator has started
  private final DataStatisticsFactory<K> statisticsFactory;
  private boolean started;

  public DataStatisticsCoordinator(String operatorName,
                            ExecutorService coordinatorExecutor,
                            ShuffleCoordinatorContext<K> context,
                                   DataStatisticsFactory<K> statisticsFactory,
                            int maxAggregateDataDistributionHistoryToKeep) {
    this.operatorName = operatorName;
    this.coordinatorExecutor = coordinatorExecutor;
    this.context = context;
    this.statisticsFactory = statisticsFactory;
    Preconditions.checkArgument(maxAggregateDataDistributionHistoryToKeep > 0,
            "maxAggregateDataDistributionHistoryToKeep %d must be positive", maxAggregateDataDistributionHistoryToKeep);
    this.maxAggregateDataDistributionHistoryToKeep = maxAggregateDataDistributionHistoryToKeep;
  }

  @Override
  public void start() throws Exception {
    LOG.info("Starting data statistics coordinator for operator {}.", operatorName);
    started = true;
  }

  @Override
  public void close() throws Exception {
    LOG.info("Closing data statistics coordinator for operator {}.", operatorName);
    try {
      if (started) {
        context.close();
      }
    } finally {
      coordinatorExecutor.shutdownNow();
      // We do not expect this to actually block for long. At this point, there should
      // be very few task running in the executor, if any.
      coordinatorExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }
  }

  private void ensureStarted() {
    if (!started) {
      throw new IllegalStateException("The coordinator has not started yet.");
    }
  }

  private void runInCoordinatorThread(final ThrowingRunnable<Throwable> action,
                                      final String actionName,
                                      final Object... actionNameFormatParameters) {
    ensureStarted();
    coordinatorExecutor.execute(
            () -> {
              try {
                action.run();
              } catch (Throwable t) {
                // if we have a JVM critical error, promote it immediately, there is a good
                // chance the logging or job failing will not succeed anymore
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

                final String actionString = String.format(actionName, actionNameFormatParameters);
                LOG.error(
                        "Uncaught exception in the shuffle coordinator for operator {} while {}. Triggering job failover.",
                        operatorName,
                        actionString,
                        t);
                context.failJob(t);
              }
            });
  }

  private void handleDataStatisticRequest(int subtask, DataStatisticsEvent<K> event) {
    long checkpointId = event.checkpointId();

    aggregateDataStatisticsMap.putIfAbsent(checkpointId, new DataDistributionWeight<>(checkpointId, statisticsFactory));
    aggregateDataStatisticsMap.get(checkpointId).addDataStatisticEvent(subtask, event);

    if (aggregateDataStatisticsMap.get(checkpointId).aggregatedSize() == context.currentParallelism()) {
      if (dataDistributionWeight == null || checkpointId >= dataDistributionWeight.checkpointId()) {
        DataDistributionWeight<K> newDataDistributionWeight = aggregateDataStatisticsMap.get(checkpointId);
        if (!newDataDistributionWeight.dataStatistics().isEmpty()) {
          dataDistributionWeight = newDataDistributionWeight;
          context.sendDataDistributionWeightToSubtasks(dataDistributionWeight);
        }
      }

      // Clean up all the aggregateDataStatistics whose checkpoint is  <= current checkpoint
      Map<Long, DataDistributionWeight<K>> toBeCleaned =  aggregateDataStatisticsMap
              .headMap(checkpointId + 1);
      aggregateDataStatisticsMap.keySet().removeAll(toBeCleaned.keySet());
    }

    // If aggregateDataStatisticsMap contains more than maxAggregateDataDistributionHistoryToKeep entries, remove them
    int toBeCleanedEntrySize = aggregateDataStatisticsMap.size() - maxAggregateDataDistributionHistoryToKeep;
    if (toBeCleanedEntrySize > 0) {
      Arrays.asList(aggregateDataStatisticsMap.keySet().toArray(new Long[0]))
              .subList(0, toBeCleanedEntrySize).forEach(aggregateDataStatisticsMap::remove);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception {
    runInCoordinatorThread(
            () -> {
              LOG.debug("Handling event from subtask {} of {}: {}",
                      subtask,
                      operatorName,
                      event);
              if (event instanceof DataStatisticsEvent) {
                handleDataStatisticRequest(subtask, ((DataStatisticsEvent<K>) event));
              } else {
                throw new FlinkException("Unrecognized shuffle operator Event: " + event);
              }
            },
            "handling operator event %s from shuffle operator subtask %d",
            event,
            subtask);
  }

  @Override
  public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
    runInCoordinatorThread(
            () -> {
              LOG.debug("Taking a state snapshot on shuffle coordinator {} for checkpoint {}",
                      operatorName, checkpointId);
              try {
                byte[] serializedDataDistributionWeight = InstantiationUtil
                        .serializeObject(dataDistributionWeight);
                resultFuture.complete(serializedDataDistributionWeight);
              } catch (Throwable e) {
                ExceptionUtils.rethrowIfFatalErrorOrOOM(e);
                resultFuture.completeExceptionally(
                        new CompletionException(
                                String.format(
                                        "Failed to checkpoint data statistics for shuffle coordinator %s",
                                        operatorName),
                                e));
              }
            },
            "taking checkpoint %d",
            checkpointId);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {

  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) {
    OperatorCoordinator.super.notifyCheckpointAborted(checkpointId);
  }

  @Override
  public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception {
    if (started) {
      throw new IllegalStateException("The coordinator can only be reset if it was not yet started");
    }

    if (checkpointData == null) {
      return;
    }

    LOG.info("Restoring data statistic weights of shuffle {} from checkpoint.", operatorName);
    dataDistributionWeight = InstantiationUtil.deserializeObject(checkpointData, Map.class.getClassLoader());
  }

  @Override
  public void subtaskReset(int subtask, long checkpointId) {

  }

  @Override
  public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {

  }

  @Override
  public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {

  }

  // ---------------------------------------------------
  @VisibleForTesting
  DataDistributionWeight<K> dataDistributionWeight() {
    return dataDistributionWeight;
  }

  @VisibleForTesting
  SortedMap<Long, DataDistributionWeight<K>> aggregateDataStatisticsMap() {
    return aggregateDataStatisticsMap;
  }

  @VisibleForTesting
  ShuffleCoordinatorContext<K> context() {
    return context;
  }
}
