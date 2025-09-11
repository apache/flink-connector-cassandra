/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.cassandra.sink.config;

import org.apache.flink.connector.cassandra.sink.planner.SinkPluggable;
import org.apache.flink.connector.cassandra.sink.planner.core.resolution.ResolutionMode;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ColumnValueResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.TableResolver;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Unit tests for {@link RowDataSinkConfig}. */
class RowDataSinkConfigTest {

    @Test
    void testRequiresNonNullDataType() {
        assertThatThrownBy(() -> RowDataSinkConfig.forRowData(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rowDataType cannot be null");
    }

    @Test
    void testStoresRowDataType() {
        DataType rowDataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP()));

        RowDataSinkConfig config = RowDataSinkConfig.forRowData(rowDataType);
        assertThat(config.getRowDataType()).isEqualTo(rowDataType);
        assertThat(config.getRecordFormatType()).isEqualTo(RecordFormatType.ROWDATA);
    }

    @Test
    void testChainingBothModes() {
        DataType rowDataType = DataTypes.ROW(DataTypes.FIELD("col1", DataTypes.STRING()));

        // Test STATIC mode
        RowDataSinkConfig staticConfig =
                RowDataSinkConfig.forRowData(rowDataType)
                        .withIgnoreNullFields(true)
                        .withQuery("INSERT INTO keyspace.table (col1) VALUES (?)");

        assertThat(staticConfig.getIgnoreNullFields()).isTrue();
        assertThat(staticConfig.getRowDataType()).isEqualTo(rowDataType);
        assertThat(staticConfig.getQuery())
                .isEqualTo("INSERT INTO keyspace.table (col1) VALUES (?)");
        assertThat(staticConfig.getResolutionMode()).isEqualTo(ResolutionMode.STATIC);

        @SuppressWarnings("unchecked")
        SinkPluggable<RowData> pluggable =
                SinkPluggable.<RowData>builder()
                        .withTableResolver(mock(TableResolver.class))
                        .withColumnValueResolver(mock(ColumnValueResolver.class))
                        .build();

        RowDataSinkConfig dynamicConfig =
                RowDataSinkConfig.forRowData(rowDataType).withPluggable(pluggable);

        assertThat(dynamicConfig.getPluggable()).isEqualTo(pluggable);
        assertThat(dynamicConfig.getRowDataType()).isEqualTo(rowDataType);
        assertThat(dynamicConfig.getResolutionMode()).isEqualTo(ResolutionMode.DYNAMIC);
    }

    @Test
    void testComplexNestedRowType() {
        // Test with nested row types
        DataType nestedRowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD(
                                "metadata",
                                DataTypes.ROW(
                                        DataTypes.FIELD("created", DataTypes.TIMESTAMP()),
                                        DataTypes.FIELD("version", DataTypes.INT()))),
                        DataTypes.FIELD("values", DataTypes.ARRAY(DataTypes.DOUBLE())));

        RowDataSinkConfig config = RowDataSinkConfig.forRowData(nestedRowType);

        assertThat(config.getRowDataType()).isEqualTo(nestedRowType);
        RowType rowType = (RowType) config.getRowDataType().getLogicalType();
        assertThat(rowType.getFieldCount()).isEqualTo(3);
        assertThat(rowType.getFieldNames()).containsExactly("id", "metadata", "values");
    }

    @Test
    void testModeConflictsWithRowData() {
        DataType rowDataType = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()));

        // Test ignoreNullFields fails in DYNAMIC mode
        @SuppressWarnings("unchecked")
        SinkPluggable<RowData> pluggable =
                SinkPluggable.<RowData>builder()
                        .withTableResolver(mock(TableResolver.class))
                        .withColumnValueResolver(mock(ColumnValueResolver.class))
                        .build();

        RowDataSinkConfig dynamicConfig =
                RowDataSinkConfig.forRowData(rowDataType).withPluggable(pluggable);

        assertThatThrownBy(() -> dynamicConfig.withIgnoreNullFields(true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ignoreNullFields is not supported in DYNAMIC mode");

        // Test mutual exclusivity
        RowDataSinkConfig staticConfig =
                RowDataSinkConfig.forRowData(rowDataType).withQuery("INSERT INTO test VALUES (?)");

        assertThatThrownBy(() -> staticConfig.withPluggable(pluggable))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot set pluggables when a query is already configured");
    }
}
