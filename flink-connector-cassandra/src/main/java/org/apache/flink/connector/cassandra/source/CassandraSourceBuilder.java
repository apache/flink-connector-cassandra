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

package org.apache.flink.connector.cassandra.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.cassandra.source.reader.converter.CassandraRowToPojoConverter;
import org.apache.flink.connector.cassandra.source.reader.converter.CassandraRowToRowDataConverter;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.Objects;

/**
 * Builder for {@link CassandraSource} that provides a fluent API for configuring Cassandra source
 * parameters.
 *
 * <h3>Usage Examples:</h3>
 *
 * <h4>POJO Source (DataStream API):</h4>
 *
 * <pre>{@code
 * CassandraSource<MyPojo> source = CassandraSource.builder()
 *     .setClusterBuilder(clusterBuilder)
 *     .setQuery("SELECT * FROM my_keyspace.my_table")
 *     .setMapperOptions(() -> new Mapper.Option[] {Mapper.Option.consistencyLevel(ANY)})
 *     .setMaxSplitMemorySize(MemorySize.ofMebiBytes(32))
 *     .forPojo(MyPojo.class);
 * }</pre>
 *
 * <h4>RowData Source (Table API - typically used internally):</h4>
 *
 * <pre>{@code
 * CassandraSource<RowData> source = CassandraSource.builder()
 *     .setClusterBuilder(clusterBuilder)
 *     .setQuery("SELECT * FROM my_keyspace.my_table")
 *     .forRowData(rowType);
 * }</pre>
 */
@PublicEvolving
public final class CassandraSourceBuilder {

    private ClusterBuilder clusterBuilder;
    private String query;
    private long maxSplitMemorySize = CassandraSource.MAX_SPLIT_MEMORY_SIZE_DEFAULT;
    private MapperOptions mapperOptions;

    CassandraSourceBuilder() {}

    /**
     * Sets the cluster builder for connecting to Cassandra cluster.
     *
     * @param clusterBuilder the cluster builder configuration
     * @return this builder instance
     */
    public CassandraSourceBuilder setClusterBuilder(ClusterBuilder clusterBuilder) {
        this.clusterBuilder = clusterBuilder;
        return this;
    }

    /**
     * Sets the CQL query to execute.
     *
     * <p>Query must be a simple SELECT statement without aggregations, ORDER BY, or GROUP BY
     * clauses, as these operations are not supported when the query is executed on data partitions.
     *
     * @param query the CQL query string
     * @return this builder instance
     */
    public CassandraSourceBuilder setQuery(String query) {
        this.query = query;
        return this;
    }

    /**
     * Sets the maximum memory size for each split. Larger tables will be divided into multiple
     * splits based on this size.
     *
     * <p>Default: 64MB, Minimum: 10MB
     *
     * @param maxSplitMemorySize the maximum memory size per split
     * @return this builder instance
     */
    public CassandraSourceBuilder setMaxSplitMemorySize(MemorySize maxSplitMemorySize) {
        this.maxSplitMemorySize = maxSplitMemorySize.getBytes();
        return this;
    }

    /**
     * Sets mapper options for POJO mapping configuration.
     *
     * <p>Optional. If not set, default mapper options will be used.
     *
     * @param mapperOptions the mapper options for DataStax object mapper
     * @return this builder instance
     */
    public CassandraSourceBuilder setMapperOptions(MapperOptions mapperOptions) {
        this.mapperOptions = mapperOptions;
        return this;
    }

    /**
     * Builds a CassandraSource configured for POJO output using DataStax object mapper.
     *
     * <p>The POJO class must be annotated with DataStax mapping annotations (e.g., {@code @Table},
     * {@code @Column}).
     *
     * @param <T> the POJO type
     * @param pojoClass the POJO class to map rows to
     * @return the configured CassandraSource instance
     * @throws IllegalStateException if required parameters are missing or invalid
     */
    public <T> CassandraSource<T> forPojo(Class<T> pojoClass) {
        validateCommonParameters();
        Objects.requireNonNull(pojoClass, "POJO class is required");

        MapperOptions options =
                mapperOptions != null
                        ? mapperOptions
                        : () -> new com.datastax.driver.mapping.Mapper.Option[0];

        CassandraRowToPojoConverter<T> converter =
                new CassandraRowToPojoConverter<>(pojoClass, options, clusterBuilder);

        return new CassandraSource<>(clusterBuilder, maxSplitMemorySize, converter, query);
    }

    /**
     * Builds a CassandraSource configured for RowData output for Table API.
     *
     * <p>This method is typically used internally by the Table API factory.
     *
     * @param rowType the logical row type definition
     * @return the configured CassandraSource instance
     * @throws IllegalStateException if required parameters are missing or invalid
     */
    public CassandraSource<RowData> forRowData(RowType rowType) {
        validateCommonParameters();
        Objects.requireNonNull(rowType, "RowType is required");

        CassandraRowToRowDataConverter converter = new CassandraRowToRowDataConverter(rowType);

        return new CassandraSource<>(clusterBuilder, maxSplitMemorySize, converter, query);
    }

    private void validateCommonParameters() {
        Objects.requireNonNull(clusterBuilder, "ClusterBuilder is required");
        Objects.requireNonNull(query, "Query is required");

        if (maxSplitMemorySize < CassandraSource.MIN_SPLIT_MEMORY_SIZE) {
            throw new IllegalArgumentException(
                    String.format(
                            "maxSplitMemorySize (%s) must be at least %s",
                            maxSplitMemorySize, CassandraSource.MIN_SPLIT_MEMORY_SIZE));
        }
    }
}
