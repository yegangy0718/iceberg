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

import java.util.Map;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class TestGlobalStatisticsAggregator {
  private final RowType rowType = RowType.of(new VarCharType());

  @Test
  public void mergeDataStatisticTest() {
    BinaryRowData binaryRowDataA =
        new RowDataSerializer(rowType).toBinaryRow(GenericRowData.of(StringData.fromString("a")));
    BinaryRowData binaryRowDataB =
        new RowDataSerializer(rowType).toBinaryRow(GenericRowData.of(StringData.fromString("b")));

    GlobalStatisticsAggregator<MapDataStatistics, Map<RowData, Long>> globalStatisticsAggregator =
        new GlobalStatisticsAggregator<>(
            1,
            MapDataStatisticsSerializer.fromKeySerializer(
                new RowDataSerializer(RowType.of(new VarCharType()))));
    MapDataStatistics mapDataStatistics1 = new MapDataStatistics();
    mapDataStatistics1.add(binaryRowDataA);
    mapDataStatistics1.add(binaryRowDataA);
    mapDataStatistics1.add(binaryRowDataB);
    globalStatisticsAggregator.mergeDataStatistic(1, 1, mapDataStatistics1);
    MapDataStatistics mapDataStatistics2 = new MapDataStatistics();
    mapDataStatistics2.add(binaryRowDataA);
    globalStatisticsAggregator.mergeDataStatistic(2, 1, mapDataStatistics2);
    globalStatisticsAggregator.mergeDataStatistic(1, 1, mapDataStatistics1);
    Assertions.assertEquals(
        mapDataStatistics1.statistics().get(binaryRowDataA)
            + mapDataStatistics2.statistics().get(binaryRowDataA),
        globalStatisticsAggregator.dataStatistics().statistics().get(binaryRowDataA));
    Assertions.assertEquals(
        mapDataStatistics1.statistics().get(binaryRowDataB)
            + mapDataStatistics2.statistics().getOrDefault(binaryRowDataB, 0L),
        globalStatisticsAggregator.dataStatistics().statistics().get(binaryRowDataB));
  }
}
