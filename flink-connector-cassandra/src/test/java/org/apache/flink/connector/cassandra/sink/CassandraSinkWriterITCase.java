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

package org.apache.flink.connector.cassandra.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.cassandra.CassandraTestEnvironment;
import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.write.RequestConfiguration;
import org.apache.flink.connector.cassandra.sink.exception.CassandraFailureHandler;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Minimal integration tests for {@link CassandraSinkWriter} with real Cassandra. Goal: prove the
 * real writer + semaphore/mailbox loop works end-to-end without deadlock. Heavy behavior testing
 * belongs in CassandraSinkWriterTest with mocks.
 */
@Testcontainers
class CassandraSinkWriterITCase {

    private static final CassandraTestEnvironment cassandraTestEnvironment =
            new CassandraTestEnvironment(false);

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

    private static final String KEYSPACE = "it_ks";
    private static final String TABLE = "it_smoke";
    private static final String CREATE_KEYSPACE_QUERY =
            "CREATE KEYSPACE IF NOT EXISTS "
                    + KEYSPACE
                    + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}";
    private static final String CREATE_TABLE_QUERY =
            "CREATE TABLE IF NOT EXISTS "
                    + KEYSPACE
                    + "."
                    + TABLE
                    + " (pk int PRIMARY KEY, v text)";
    private static final String INSERT_QUERY =
            "INSERT INTO " + KEYSPACE + "." + TABLE + " (pk, v) VALUES (?, ?)";
    private static final String SELECT_COUNT_QUERY =
            "SELECT COUNT(*) FROM " + KEYSPACE + "." + TABLE;
    private static final String TRUNCATE_QUERY = "TRUNCATE " + KEYSPACE + "." + TABLE;

    private static Session session;
    private ClusterBuilder clusterBuilder;
    private SinkWriterMetricGroup metricGroup;
    private SyncMailboxExecutor mailboxExecutor;

    @BeforeAll
    static void setupCassandra() throws Exception {
        cassandraTestEnvironment.startUp();
        session = cassandraTestEnvironment.getSession();
        session.execute(CREATE_KEYSPACE_QUERY);
        session.execute(CREATE_TABLE_QUERY);
    }

    @AfterAll
    static void tearDownCassandra() throws Exception {
        if (session != null) {
            session.close();
        }
        cassandraTestEnvironment.tearDown();
    }

    @BeforeEach
    void setUp() throws Exception {
        try {
            session.execute(TRUNCATE_QUERY);
        } catch (Exception ignored) {
        }
        clusterBuilder =
                new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoint(cassandraTestEnvironment.getHost())
                                .withPort(cassandraTestEnvironment.getPort())
                                .build();
                    }
                };

        setupMetricGroup();
        mailboxExecutor = new SyncMailboxExecutor();
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            session.execute(TRUNCATE_QUERY);
        } catch (Exception ignored) {

        }
    }

    @Test
    void testBackpressureSmoke_maxConcurrent1() throws Exception {
        RequestConfiguration requestConfig =
                RequestConfiguration.builder()
                        .setMaxConcurrentRequests(1)
                        .setMaxRetries(1)
                        .setMaxTimeout(Duration.ofSeconds(10))
                        .build();

        CqlSinkConfig<Row> config = CqlSinkConfig.<Row>forRow().withQuery(INSERT_QUERY);

        try (CassandraSinkWriter<Row> sinkWriter =
                new CassandraSinkWriter<>(
                        requestConfig,
                        config,
                        clusterBuilder,
                        new CassandraFailureHandler(),
                        mailboxExecutor,
                        metricGroup)) {

            // When you fire 3-4 writes quickly, then flush()
            sinkWriter.write(Row.of(1, "v1"), mock(SinkWriter.Context.class));
            sinkWriter.write(Row.of(2, "v2"), mock(SinkWriter.Context.class));
            sinkWriter.write(Row.of(3, "v3"), mock(SinkWriter.Context.class));
            sinkWriter.write(Row.of(4, "v4"), mock(SinkWriter.Context.class));

            sinkWriter.flush(false);
            assertThat(getRowCount()).isEqualTo(4);
        }
    }

    @Test
    void testFlushWaitsForCompletion() throws Exception {
        // Given writer with maxConcurrentRequests=2
        RequestConfiguration requestConfig =
                RequestConfiguration.builder()
                        .setMaxConcurrentRequests(2)
                        .setMaxRetries(1)
                        .setMaxTimeout(Duration.ofSeconds(10))
                        .build();

        CqlSinkConfig<Row> config = CqlSinkConfig.<Row>forRow().withQuery(INSERT_QUERY);

        try (CassandraSinkWriter<Row> sinkWriter =
                new CassandraSinkWriter<>(
                        requestConfig,
                        config,
                        clusterBuilder,
                        new CassandraFailureHandler(),
                        mailboxExecutor,
                        metricGroup)) {
            int k = 5;
            for (int i = 0; i < k; i++) {
                sinkWriter.write(Row.of(i, "value" + i), mock(SinkWriter.Context.class));
            }

            sinkWriter.flush(false);

            // Then after flush(), SELECT COUNT(*) = k
            assertThat(getRowCount()).isEqualTo(k);
        }
    }

    @Test
    void testCloseIsIdempotent() throws Exception {
        // Given some writes in flight
        RequestConfiguration requestConfig =
                RequestConfiguration.builder()
                        .setMaxConcurrentRequests(2)
                        .setMaxRetries(1)
                        .setMaxTimeout(Duration.ofSeconds(10))
                        .build();

        CqlSinkConfig<Row> config = CqlSinkConfig.<Row>forRow().withQuery(INSERT_QUERY);

        CassandraSinkWriter<Row> sinkWriter =
                new CassandraSinkWriter<>(
                        requestConfig,
                        config,
                        clusterBuilder,
                        new CassandraFailureHandler(),
                        mailboxExecutor,
                        metricGroup);

        sinkWriter.write(Row.of(1, "test1"), mock(SinkWriter.Context.class));
        sinkWriter.write(Row.of(2, "test2"), mock(SinkWriter.Context.class));

        // When close() then close() again
        sinkWriter.close();
        sinkWriter.close(); // Should be idempotent

        // Then no exception; rows are persisted
        assertThat(getRowCount()).isEqualTo(2);

        // And subsequent write() throws (closed state) - expect RuntimeException due to closed
        // cluster
        assertThatThrownBy(
                        () -> sinkWriter.write(Row.of(3, "fail"), mock(SinkWriter.Context.class)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot execute mail");
    }

    @Test
    void testFatalErrorSurfacesOnce() throws Exception {
        // Given config targeting a non-existent table
        String badInsertQuery = "INSERT INTO " + KEYSPACE + ".non_existent (pk, v) VALUES (?, ?)";

        RequestConfiguration requestConfig =
                RequestConfiguration.builder()
                        .setMaxConcurrentRequests(1)
                        .setMaxRetries(1)
                        .setMaxTimeout(Duration.ofSeconds(5))
                        .build();

        CqlSinkConfig<Row> config = CqlSinkConfig.<Row>forRow().withQuery(badInsertQuery);

        try (CassandraSinkWriter<Row> sinkWriter =
                new CassandraSinkWriter<>(
                        requestConfig,
                        config,
                        clusterBuilder,
                        new CassandraFailureHandler(),
                        mailboxExecutor,
                        metricGroup)) {

            assertThatThrownBy(
                            () ->
                                    sinkWriter.write(
                                            Row.of(1, "test"), mock(SinkWriter.Context.class)))
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasRootCauseInstanceOf(InvalidQueryException.class);
        }
    }

    private long getRowCount() {
        ResultSet result = session.execute(SELECT_COUNT_QUERY);
        return result.one().getLong(0);
    }

    private void setupMetricGroup() {
        metricGroup = mock(SinkWriterMetricGroup.class);
        OperatorIOMetricGroup ioMetricGroup = mock(OperatorIOMetricGroup.class);

        when(metricGroup.getIOMetricGroup()).thenReturn(ioMetricGroup);
        when(ioMetricGroup.getNumRecordsOutCounter()).thenReturn(new SimpleCounter());
        when(metricGroup.getNumRecordsOutErrorsCounter()).thenReturn(new SimpleCounter());
        when(metricGroup.counter(any())).thenReturn(new SimpleCounter());
        when(metricGroup.histogram(any(), any())).thenReturn(mock(Histogram.class));
    }

    /** Simple counter for metrics. */
    private static class SimpleCounter implements Counter {
        private long count = 0;

        @Override
        public void inc() {
            count++;
        }

        @Override
        public void inc(long n) {
            count += n;
        }

        @Override
        public void dec() {
            count--;
        }

        @Override
        public void dec(long n) {
            count -= n;
        }

        @Override
        public long getCount() {
            return count;
        }
    }
}
