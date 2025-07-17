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

package org.apache.flink.connector.cassandra.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.cassandra.source.CassandraSource;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Objects;

/** A {@link DynamicTableSource} for Cassandra that creates a specialized table source. */
@Internal
public class CassandraDynamicTableSource implements ScanTableSource {

    private final ClusterBuilder clusterBuilder;
    private final String keyspace;
    private final String table;
    private final DataType producedDataType;
    private final String query;
    private final long maxSplitMemorySize;

    public CassandraDynamicTableSource(
            ClusterBuilder clusterBuilder,
            String keyspace,
            String table,
            DataType producedDataType,
            String query,
            long maxSplitMemorySize) {
        this.clusterBuilder = clusterBuilder;
        this.keyspace = keyspace;
        this.table = table;
        this.producedDataType = producedDataType;
        this.query = query;
        this.maxSplitMemorySize = maxSplitMemorySize;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanTableSource.ScanContext context) {
        final RowType rowType = (RowType) producedDataType.getLogicalType();

        CassandraSource<RowData> cassandraSource =
                CassandraSource.builder()
                        .setClusterBuilder(clusterBuilder)
                        .setQuery(query)
                        .setMaxSplitMemorySize(MemorySize.parse(maxSplitMemorySize + "b"))
                        .forRowData(rowType);

        return SourceProvider.of(cassandraSource);
    }

    @Override
    public DynamicTableSource copy() {
        return new CassandraDynamicTableSource(
                clusterBuilder, keyspace, table, producedDataType, query, maxSplitMemorySize);
    }

    @Override
    public String asSummaryString() {
        return "Cassandra";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraDynamicTableSource that = (CassandraDynamicTableSource) o;
        return maxSplitMemorySize == that.maxSplitMemorySize
                && Objects.equals(clusterBuilder, that.clusterBuilder)
                && Objects.equals(keyspace, that.keyspace)
                && Objects.equals(table, that.table)
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                clusterBuilder, keyspace, table, producedDataType, query, maxSplitMemorySize);
    }
}
