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
import org.apache.flink.streaming.connectors.cassandra.CassandraMapperOptions;
import org.apache.flink.streaming.connectors.cassandra.CassandraSessionBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder for {@link CassandraSource} that provides a fluent API for configuring Cassandra source
 * parameters.
 *
 * <h3>Usage Examples:</h3>
 *
 * <h4>POJO Source (DataStream API):</h4>
 *
 * <pre>{@code
 * // See CassandraSource class documentation for CassandraSessionBuilder creation examples
 * CassandraSessionBuilder sessionBuilder = ...; // your CassandraSessionBuilder implementation
 *
 * CassandraSource<MyPojo> source = CassandraSource.builder()
 *     .setSessionBuilder(sessionBuilder)
 *     .setQuery("SELECT * FROM my_keyspace.my_table")
 *     .setMaxSplitMemorySize(MemorySize.ofMebiBytes(32))
 *     .buildForPojo(MyPojo.class);
 * }</pre>
 *
 * <h3>Consistency Level Configuration:</h3>
 *
 * <p>Set the consistency level in the driver configuration of the {@link CassandraSessionBuilder},
 * it applies to all the queries executed by this source. See {@link CassandraSource} class
 * documentation for complete CassandraSessionBuilder examples.
 *
 * <h4>RowData Source (Table API - typically used internally):</h4>
 *
 * <pre>{@code
 * // See CassandraSource class documentation for CassandraSessionBuilder creation examples
 * CassandraSessionBuilder sessionBuilder = ...; // your CassandraSessionBuilder implementation
 *
 * CassandraSource<RowData> source = CassandraSource.builder()
 *     .setSessionBuilder(sessionBuilder)
 *     .setQuery("SELECT * FROM my_keyspace.my_table")
 *     .buildForRowData(rowType);
 * }</pre>
 */
@PublicEvolving
public final class CassandraSourceBuilder {

    private CassandraSessionBuilder sessionBuilder;
    private String query;
    private long maxSplitMemorySize = CassandraSource.MAX_SPLIT_MEMORY_SIZE_DEFAULT;
    private CassandraMapperOptions mapperOptions;

    CassandraSourceBuilder() {}

    /**
     * Sets the cluster builder for connecting to Cassandra cluster.
     *
     * @param sessionBuilder the cluster builder configuration
     * @return this builder instance
     */
    public CassandraSourceBuilder setSessionBuilder(CassandraSessionBuilder sessionBuilder) {
        this.sessionBuilder = sessionBuilder;
        return this;
    }

    /**
     * Sets the CQL query to execute.
     *
     * <p>See {@link CassandraSource} class documentation for query limitations and requirements.
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
     * <p>Optional. The mapper options only configure how POJOs are written, they have no effect on
     * this source.
     *
     * @param mapperOptions the mapper options
     * @return this builder instance
     */
    public CassandraSourceBuilder setMapperOptions(CassandraMapperOptions mapperOptions) {
        this.mapperOptions = mapperOptions;
        return this;
    }

    /**
     * Builds a {@link CassandraSource} configured for POJO output.
     *
     * <p>The POJO class must be annotated with the Cassandra Java driver mapper annotations (e.g.,
     * {@code @Entity}, {@code @CqlName}).
     *
     * @param <T> the POJO type
     * @param pojoClass the POJO class to map rows to
     * @return the configured CassandraSource instance
     * @throws IllegalStateException if required parameters are missing or invalid
     */
    public <T> CassandraSource<T> buildForPojo(Class<T> pojoClass) {
        validateCommonParameters();
        checkNotNull(pojoClass, "POJO class is required");

        CassandraRowToPojoConverter<T> converter = new CassandraRowToPojoConverter<>(pojoClass);

        return new CassandraSource<>(sessionBuilder, maxSplitMemorySize, converter, query);
    }

    /**
     * Builds a {@link CassandraSource} configured for RowData output for Table API.
     *
     * <p>This method is typically used internally by the Table API factory.
     *
     * @param rowType the logical row type definition
     * @return the configured CassandraSource instance
     * @throws IllegalStateException if required parameters are missing or invalid
     */
    public CassandraSource<RowData> buildForRowData(RowType rowType) {
        validateCommonParameters();
        checkNotNull(rowType, "RowType is required");

        CassandraRowToRowDataConverter converter = new CassandraRowToRowDataConverter(rowType);

        return new CassandraSource<>(sessionBuilder, maxSplitMemorySize, converter, query);
    }

    private void validateCommonParameters() {
        checkNotNull(sessionBuilder, "CassandraSessionBuilder is required");
        checkNotNull(query, "Query is required");

        if (maxSplitMemorySize < CassandraSource.MIN_SPLIT_MEMORY_SIZE) {
            throw new IllegalArgumentException(
                    String.format(
                            "maxSplitMemorySize (%s) must be at least %s",
                            maxSplitMemorySize, CassandraSource.MIN_SPLIT_MEMORY_SIZE));
        }
    }
}
