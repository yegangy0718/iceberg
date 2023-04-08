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
 * DataStatisticsCoordinator receives {@link DataStatisticsEvent} from {@link
 * DataStatisticsOperator} every subtask and then merge them together. Once aggregation for all
 * subtasks data statistics completes, DataStatisticsCoordinator will send the aggregation
 * result(global data statistics) back to {@link DataStatisticsOperator}. In the end a custom
 * partitioner will distribute traffic based on the global data statistics to improve data
 * clustering.
 */
class DataStatisticsCoordinator<K> implements OperatorCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(DataStatisticsCoordinator.class);

  private final String operatorName;
  // A single-thread executor to handle all the actions for coordinator
  private final ExecutorService coordinatorExecutor;
  private final DataStatisticsCoordinatorContext<K> context;
  // The max size for aggregateDataStatisticsMap which is the max aggregateDataStatistics history
  // to keep based on checkpoint
  private final int maxAggregateDataDistributionHistoryToKeep;
  // key is the checkpoint id, value is the AggregatedDataStatistics at the corresponding checkpoint
  private final SortedMap<Long, AggregatedDataStatistics<K>> pendingAggregatedDataStatisticsMap =
      new TreeMap<>();
  // the latest AggregatedDataStatistics which is sent to operators
  private volatile AggregatedDataStatistics<K> completedAggregatedDataStatistics;
  private final DataStatisticsFactory<K> statisticsFactory;
  private boolean started;

  public DataStatisticsCoordinator(
      String operatorName,
      ExecutorService coordinatorExecutor,
      DataStatisticsCoordinatorContext<K> context,
      DataStatisticsFactory<K> statisticsFactory,
      int maxAggregateDataDistributionHistoryToKeep) {
    this.operatorName = operatorName;
    this.coordinatorExecutor = coordinatorExecutor;
    this.context = context;
    this.statisticsFactory = statisticsFactory;
    Preconditions.checkArgument(
        maxAggregateDataDistributionHistoryToKeep > 0,
        "maxAggregateDataDistributionHistoryToKeep %d must be positive",
        maxAggregateDataDistributionHistoryToKeep);
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

  private void runInCoordinatorThread(
      final ThrowingRunnable<Throwable> action,
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
                "Uncaught exception in the data statistics coordinator for operator {} while {}. Triggering job failover.",
                operatorName,
                actionString,
                t);
            context.failJob(t);
          }
        });
  }

  private void ensureStarted() {
    if (!this.started) {
      throw new IllegalStateException("The coordinator has not started yet.");
    }
  }

  private void handleDataStatisticRequest(int subtask, DataStatisticsEvent<K> event) {
    long checkpointId = event.checkpointId();

    pendingAggregatedDataStatisticsMap.putIfAbsent(
        checkpointId, new AggregatedDataStatistics<>(checkpointId, statisticsFactory));
    pendingAggregatedDataStatisticsMap.get(checkpointId).mergeDataStatistic(subtask, event);

    if (pendingAggregatedDataStatisticsMap.get(checkpointId).aggregatedSize()
        == context.currentParallelism()) {
      if (completedAggregatedDataStatistics == null
          || checkpointId >= completedAggregatedDataStatistics.checkpointId()) {
        AggregatedDataStatistics<K> newCompletedAggregatedDataStatistics =
            pendingAggregatedDataStatisticsMap.get(checkpointId);
        if (!newCompletedAggregatedDataStatistics.dataStatistics().isEmpty()) {
          completedAggregatedDataStatistics = newCompletedAggregatedDataStatistics;
          context.sendDataStatisticsToSubtasks(
              checkpointId, completedAggregatedDataStatistics.dataStatistics());
        }
      }

      // Clean up all the aggregateDataStatistics whose checkpoint is  <= completed aggregated
      // checkpoint
      Map<Long, AggregatedDataStatistics<K>> toBeCleaned =
          pendingAggregatedDataStatisticsMap.headMap(checkpointId + 1);
      pendingAggregatedDataStatisticsMap.keySet().removeAll(toBeCleaned.keySet());
    }

    // If aggregateDataStatisticsMap contains more than maxAggregateDataDistributionHistoryToKeep
    // entries, remove them
    int toBeCleanedEntrySize =
        pendingAggregatedDataStatisticsMap.size() - maxAggregateDataDistributionHistoryToKeep;
    if (toBeCleanedEntrySize > 0) {
      Arrays.asList(pendingAggregatedDataStatisticsMap.keySet().toArray(new Long[0]))
          .subList(0, toBeCleanedEntrySize)
          .forEach(pendingAggregatedDataStatisticsMap::remove);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
    runInCoordinatorThread(
        () -> {
          LOG.debug("Handling event from subtask {} of {}: {}", subtask, operatorName, event);
          if (event instanceof DataStatisticsEvent) {
            handleDataStatisticRequest(subtask, ((DataStatisticsEvent<K>) event));
          } else {
            throw new FlinkException("Unrecognized data statistics operator event: " + event);
          }
        },
        "handling operator event %s from data statistics operator subtask %d",
        event,
        subtask);
  }

  @Override
  public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) {
    runInCoordinatorThread(
        () -> {
          LOG.debug(
              "Taking a state snapshot on data statistics coordinator {} for checkpoint {}",
              operatorName,
              checkpointId);
          try {
            byte[] serializedDataDistributionWeight =
                InstantiationUtil.serializeObject(completedAggregatedDataStatistics);
            resultFuture.complete(serializedDataDistributionWeight);
          } catch (Throwable e) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(e);
            resultFuture.completeExceptionally(
                new CompletionException(
                    String.format(
                        "Failed to checkpoint data statistics for data statistics coordinator %s",
                        operatorName),
                    e));
          }
        },
        "taking checkpoint %d",
        checkpointId);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {}

  @Override
  public void notifyCheckpointAborted(long checkpointId) {
    OperatorCoordinator.super.notifyCheckpointAborted(checkpointId);
  }

  @Override
  public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
      throws Exception {
    if (started) {
      throw new IllegalStateException(
          "The coordinator can only be reset if it was not yet started");
    }

    if (checkpointData == null) {
      return;
    }

    LOG.info(
        "Restoring data statistic coordinator {} from checkpoint {}.", operatorName, checkpointId);
    completedAggregatedDataStatistics =
        InstantiationUtil.deserializeObject(
            checkpointData, AggregatedDataStatistics.class.getClassLoader());
  }

  @Override
  public void subtaskReset(int subtask, long checkpointId) {
    this.runInCoordinatorThread(
        () -> {
          LOG.info(
              "Resetting subtask {} to checkpoint {} for data statistics {} to checkpoint.",
              subtask,
              checkpointId,
              this.operatorName);
          this.context.subtaskReset(subtask);
        },
        "handling subtask %d recovery to checkpoint %d",
        subtask,
        checkpointId);
  }

  @Override
  public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
    this.runInCoordinatorThread(
        () -> {
          LOG.info(
              "Unregistering gateway after failure for subtask {} (#{}) of data statistic {}.",
              subtask,
              attemptNumber,
              this.operatorName);
          this.context.attemptFailed(subtask, attemptNumber);
        },
        "handling subtask %d (#%d) failure",
        subtask,
        attemptNumber);
  }

  @Override
  public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
    Preconditions.checkArgument(subtask == gateway.getSubtask());
    Preconditions.checkArgument(attemptNumber == gateway.getExecution().getAttemptNumber());
    this.runInCoordinatorThread(
        () -> {
          this.context.attemptReady(gateway);
        },
        "making event gateway to subtask %d (#%d) available",
        subtask,
        attemptNumber);
  }

  // ---------------------------------------------------
  @VisibleForTesting
  AggregatedDataStatistics<K> latestAggregatedDataStatistics() {
    return completedAggregatedDataStatistics;
  }

  @VisibleForTesting
  SortedMap<Long, AggregatedDataStatistics<K>> aggregateDataStatisticsMap() {
    return pendingAggregatedDataStatisticsMap;
  }

  @VisibleForTesting
  DataStatisticsCoordinatorContext<K> context() {
    return context;
  }
}
