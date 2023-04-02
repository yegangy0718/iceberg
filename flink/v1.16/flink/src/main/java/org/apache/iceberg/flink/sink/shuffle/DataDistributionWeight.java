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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.flink.sink.shuffle.statistics.DataStatistics;
import org.apache.iceberg.flink.sink.shuffle.statistics.DataStatisticsFactory;

class DataDistributionWeight<K> implements Serializable {

    private final long checkpointId;
    private final DataStatisticsFactory<K> statisticsFactory;
    private final DataStatistics<K> dataStatistics;
    private final Set<Integer> subtaskSet = new HashSet<>();

    DataDistributionWeight(long checkpoint, final DataStatisticsFactory<K> statisticsFactory) {
        this.checkpointId = checkpoint;
        this.statisticsFactory = statisticsFactory;
        this.dataStatistics = statisticsFactory.createDataStatistics();
    }

    long checkpointId() {
        return checkpointId;
    }

    DataStatistics<K> dataStatistics() {
        return dataStatistics;
    }

    void addDataStatisticEvent(int subtask, DataStatisticsEvent<K> event) {
        if (!subtaskSet.add(subtask)) {
            return;
        }

        dataStatistics.merge(event.dataStatistics());
    }

    long aggregatedSize() {
        return subtaskSet.size();
    }
}
