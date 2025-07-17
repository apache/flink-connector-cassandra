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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.cassandra.source.enumerator.CassandraEnumeratorState;
import org.apache.flink.connector.cassandra.source.enumerator.CassandraEnumeratorStateSerializer;
import org.apache.flink.connector.cassandra.source.enumerator.CassandraSplitEnumerator;
import org.apache.flink.connector.cassandra.source.reader.CassandraSourceReaderFactory;
import org.apache.flink.connector.cassandra.source.reader.converter.CassandraRowToPojoConverter;
import org.apache.flink.connector.cassandra.source.reader.converter.CassandraRowToRowDataConverter;
import org.apache.flink.connector.cassandra.source.reader.converter.CassandraRowToTypeConverter;
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;
import org.apache.flink.connector.cassandra.source.split.CassandraSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;

import com.datastax.driver.mapping.MappingManager;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A bounded source to read from Cassandra and return data as a {@code DataStream<OUT>} where OUT
 * can be any type determined by the provided converter. This source uses a pluggable converter
 * interface that supports both {@link org.apache.flink.table.api.Table} and {@link
 * org.apache.flink.streaming.api.datastream.DataStream}:
 *
 * <ul>
 *   <li><b>DataStream API with POJOs</b>: Uses {@link CassandraRowToPojoConverter} with Cassandra's
 *       {@link MappingManager} to convert cassandra's {@link com.datastax.driver.core.Row} to
 *       annotated POJOs
 *   <li><b>Table/SQL API</b>: Uses {@link CassandraRowToRowDataConverter} with field-based mapping
 *       to convert rows to {@link org.apache.flink.table.data.RowData}
 * </ul>
 *
 * <h3>Usage Examples:</h3>
 *
 * <h4>1. DataStream API with POJO (Object Mapper):</h4>
 *
 * <p>For DataStream applications using POJOs with Cassandra annotations:
 *
 * <pre>{@code
 * ClusterBuilder clusterBuilder = new ClusterBuilder() {
 *   @Override
 *   protected Cluster buildCluster(Cluster.Builder builder) {
 *     return builder.addContactPointsWithPorts(new InetSocketAddress(HOST, PORT))
 *                   .withQueryOptions(new QueryOptions().setConsistencyLevel(CL))
 *                   .build();
 *   }
 * };
 *
 * CassandraSource<MyPojo> source = CassandraSource.builder()
 *     .setClusterBuilder(clusterBuilder)
 *     .setQuery("SELECT * FROM my_keyspace.my_table")
 *     .setMapperOptions(() -> new Mapper.Option[] {Mapper.Option.saveNullFields(true)})
 *     .buildForPojo(MyPojo.class);
 *
 * DataStream<MyPojo> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "CassandraSource");
 * }</pre>
 *
 * <h4>2. Table/SQL API with RowData (Field Mapping):</h4>
 *
 * <p>For Table API and SQL applications, users create tables via SQL DDL. The factory automatically
 * creates and configures the CassandraSource internally:
 *
 * <pre>{@code
 * // Create Cassandra table using SQL DDL with custom ClusterBuilder
 * tableEnv.executeSql(
 *     "CREATE TABLE users (" +
 *     "  id INT," +
 *     "  name STRING," +
 *     "  age INT," +
 *     "  address ROW<street STRING, city STRING, zipcode INT>" +
 *     ") WITH (" +
 *     "  'connector' = 'cassandra'," +
 *     "  'hosts' = 'localhost:9042'," +
 *     "  'keyspace' = 'my_keyspace'," +
 *     "  'table' = 'users'," +
 *     "  'cluster-builder-class' = 'com.example.MyCustomClusterBuilder'" +
 *     ")"
 * );
 *
 * // Query using SQL - CassandraSource is created automatically by the factory
 * Table result = tableEnv.sqlQuery("SELECT name, age FROM users WHERE age > 25");
 * }</pre>
 *
 * <h3>Performance and Splitting:</h3>
 *
 * <p>The source automatically splits table data for parallel processing: numSplits =
 * tableSize/maxSplitMemorySize. If tableSize cannot be determined, it falls back to
 * numSplits=parallelism. Default maxSplitMemorySize is 64MB, minimum is 10MB.
 *
 * <h3>Query Limitations:</h3>
 *
 * <p>Queries must be simple SELECT statements without aggregations, ORDER BY, or GROUP BY clauses,
 * as these operations are not supported when the query is executed on data partitions.
 */
@PublicEvolving
public class CassandraSource<OUT>
        implements Source<OUT, CassandraSplit, CassandraEnumeratorState>, ResultTypeQueryable<OUT> {

    public static final Pattern CQL_PROHIBITED_CLAUSES_REGEXP =
            Pattern.compile(
                    "(?i).*(AVG\\(|COUNT\\(|MIN\\(|MAX\\(|SUM\\(|ORDER BY\\s|GROUP BY\\s).*");
    public static final Pattern SELECT_REGEXP =
            Pattern.compile("(?i)select .+ from (\\w+)\\.(\\w+).*;$");

    private static final long serialVersionUID = 1L;

    private final ClusterBuilder clusterBuilder;
    private final String query;
    private final String keyspace;
    private final String table;
    private final CassandraRowToTypeConverter<OUT> rowToTypeConverter;
    private final long maxSplitMemorySize;
    static final long MIN_SPLIT_MEMORY_SIZE = MemorySize.ofMebiBytes(10).getBytes();
    static final long MAX_SPLIT_MEMORY_SIZE_DEFAULT = MemorySize.ofMebiBytes(64).getBytes();

    /**
     * Creates a new builder for configuring a CassandraSource.
     *
     * @return a new CassandraSourceBuilder instance
     */
    public static CassandraSourceBuilder builder() {
        return new CassandraSourceBuilder();
    }

    CassandraSource(
            ClusterBuilder clusterBuilder,
            CassandraRowToTypeConverter<OUT> rowToTypeConverter,
            String query) {
        this(clusterBuilder, MAX_SPLIT_MEMORY_SIZE_DEFAULT, rowToTypeConverter, query);
    }

    CassandraSource(
            ClusterBuilder clusterBuilder,
            long maxSplitMemorySize,
            CassandraRowToTypeConverter<OUT> rowToTypeConverter,
            String query) {
        Objects.requireNonNull(clusterBuilder, "ClusterBuilder required but not provided");
        Objects.requireNonNull(rowToTypeConverter, "Row converter required but not provided");
        Objects.requireNonNull(query, "query required but not provided");
        if (maxSplitMemorySize < MIN_SPLIT_MEMORY_SIZE) {
            throw new IllegalArgumentException(
                    String.format(
                            "Defined maxSplitMemorySize (%s) is below minimum (%s)",
                            maxSplitMemorySize, MIN_SPLIT_MEMORY_SIZE));
        }
        this.maxSplitMemorySize = maxSplitMemorySize;
        final Matcher queryMatcher = checkQueryValidity(query);
        this.query = query;
        this.keyspace = queryMatcher.group(1);
        this.table = queryMatcher.group(2);
        this.clusterBuilder = clusterBuilder;
        ClosureCleaner.clean(clusterBuilder, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        this.rowToTypeConverter = rowToTypeConverter;
    }

    /** @deprecated Use {@link #builder()} instead. */
    @Deprecated
    public CassandraSource(
            ClusterBuilder clusterBuilder,
            Class<OUT> pojoClass,
            String query,
            MapperOptions mapperOptions) {
        this(clusterBuilder, MAX_SPLIT_MEMORY_SIZE_DEFAULT, pojoClass, query, mapperOptions);
    }

    /** @deprecated Use {@link #builder()} instead. */
    @Deprecated
    public CassandraSource(
            ClusterBuilder clusterBuilder,
            long maxSplitMemorySize,
            Class<OUT> pojoClass,
            String query,
            MapperOptions mapperOptions) {
        checkNotNull(clusterBuilder, "ClusterBuilder required but not provided");
        checkNotNull(pojoClass, "POJO class required but not provided");
        checkNotNull(query, "query required but not provided");
        checkState(
                maxSplitMemorySize >= MIN_SPLIT_MEMORY_SIZE,
                "Defined maxSplitMemorySize (%s) is below minimum (%s)",
                maxSplitMemorySize,
                MIN_SPLIT_MEMORY_SIZE);
        this.maxSplitMemorySize = maxSplitMemorySize;
        final Matcher queryMatcher = checkQueryValidity(query);
        this.query = query;
        this.keyspace = queryMatcher.group(1);
        this.table = queryMatcher.group(2);
        this.clusterBuilder = clusterBuilder;
        ClosureCleaner.clean(clusterBuilder, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        this.rowToTypeConverter =
                new CassandraRowToPojoConverter<>(pojoClass, mapperOptions, clusterBuilder);
    }

    @VisibleForTesting
    public static Matcher checkQueryValidity(String query) {
        checkState(
                !query.matches(CQL_PROHIBITED_CLAUSES_REGEXP.pattern()),
                "Aggregations/OrderBy are not supported because the query is executed on subsets/partitions of the input table");
        final Matcher queryMatcher = SELECT_REGEXP.matcher(query);
        checkState(
                queryMatcher.matches(),
                "Query must be of the form select ... from keyspace.table ...;");
        return queryMatcher;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Internal
    @Override
    public SourceReader<OUT, CassandraSplit> createReader(SourceReaderContext readerContext) {
        return new CassandraSourceReaderFactory<OUT>()
                .create(readerContext, clusterBuilder, rowToTypeConverter, query, keyspace, table);
    }

    @Internal
    @Override
    public SplitEnumerator<CassandraSplit, CassandraEnumeratorState> createEnumerator(
            SplitEnumeratorContext<CassandraSplit> enumContext) {
        return new CassandraSplitEnumerator(
                enumContext, null, clusterBuilder, maxSplitMemorySize, keyspace, table);
    }

    @Internal
    @Override
    public SplitEnumerator<CassandraSplit, CassandraEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<CassandraSplit> enumContext,
            CassandraEnumeratorState enumCheckpoint) {
        return new CassandraSplitEnumerator(
                enumContext, enumCheckpoint, clusterBuilder, maxSplitMemorySize, keyspace, table);
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<CassandraSplit> getSplitSerializer() {
        return CassandraSplitSerializer.INSTANCE;
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<CassandraEnumeratorState> getEnumeratorCheckpointSerializer() {
        return CassandraEnumeratorStateSerializer.INSTANCE;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return rowToTypeConverter.getProducedType();
    }
}
