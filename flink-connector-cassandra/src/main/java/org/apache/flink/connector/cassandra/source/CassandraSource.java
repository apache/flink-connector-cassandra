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
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;
import org.apache.flink.connector.cassandra.source.split.CassandraSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A bounded source to read from Cassandra and return a collection of entities as {@code
 * DataStream<Entity>}. An entity is built by Cassandra mapper ({@code
 * com.datastax.driver.mapping.EntityMapper}) based on a POJO containing annotations (as described
 * in <a
 * href="https://docs.datastax.com/en/developer/java-driver/3.11/manual/object_mapper/creating/">
 * Cassandra object mapper</a>).
 *
 * <p>To use it, do the following:
 *
 * <pre>{@code
 * ClusterBuilder clusterBuilder = new ClusterBuilder() {
 *   @Override
 *   protected Cluster buildCluster(Cluster.Builder builder) {
 *     return builder.addContactPointsWithPorts(new InetSocketAddress(HOST,PORT))
 *                   .withQueryOptions(new QueryOptions().setConsistencyLevel(CL))
 *                   .withSocketOptions(new SocketOptions()
 *                   .setConnectTimeoutMillis(CONNECT_TIMEOUT)
 *                   .setReadTimeoutMillis(READ_TIMEOUT))
 *                   .build();
 *   }
 * };
 * long maxSplitMemorySize = ... //optional max split size in bytes minimum is 10MB. If not set, maxSplitMemorySize = 64 MB
 * Source cassandraSource = new CassandraSource(clusterBuilder,
 *                                              maxSplitMemorySize,
 *                                              Pojo.class,
 *                                              "select ... from KEYSPACE.TABLE ...;",
 *                                              () -> new Mapper.Option[] {Mapper.Option.saveNullFields(true)});
 *
 * DataStream<Pojo> stream = env.fromSource(cassandraSource, WatermarkStrategy.noWatermarks(),
 * "CassandraSource");
 * }</pre>
 *
 * <p>Regarding performances, the source splits table data like this: numSplits =
 * tableSize/maxSplitMemorySize. If tableSize cannot be determined or previous numSplits computation
 * makes too few splits, falling back to numSplits=parallelism
 */
@PublicEvolving
public class CassandraSource<OUT>
        implements Source<OUT, CassandraSplit, CassandraEnumeratorState>, ResultTypeQueryable<OUT> {

    public static final Pattern CQL_PROHIBITED_CLAUSES_REGEXP =
            Pattern.compile("(?i).*(AVG|COUNT|MIN|MAX|SUM|ORDER BY|GROUP BY).*");
    public static final Pattern SELECT_REGEXP =
            Pattern.compile("(?i)select .+ from (\\w+)\\.(\\w+).*;$");

    private static final long serialVersionUID = 1L;

    private final ClusterBuilder clusterBuilder;
    private final Class<OUT> pojoClass;
    private final String query;
    private final String keyspace;
    private final String table;
    private final MapperOptions mapperOptions;

    private final long maxSplitMemorySize;
    private static final long MIN_SPLIT_MEMORY_SIZE = MemorySize.ofMebiBytes(10).getBytes();
    static final long MAX_SPLIT_MEMORY_SIZE_DEFAULT = MemorySize.ofMebiBytes(64).getBytes();

    public CassandraSource(
            ClusterBuilder clusterBuilder,
            Class<OUT> pojoClass,
            String query,
            MapperOptions mapperOptions) {
        this(clusterBuilder, MAX_SPLIT_MEMORY_SIZE_DEFAULT, pojoClass, query, mapperOptions);
    }

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
        this.pojoClass = pojoClass;
        this.mapperOptions = mapperOptions;
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
                .create(
                        readerContext,
                        clusterBuilder,
                        pojoClass,
                        query,
                        keyspace,
                        table,
                        mapperOptions);
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
        return TypeInformation.of(pojoClass);
    }
}
