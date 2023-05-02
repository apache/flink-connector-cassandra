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

package org.apache.flink.connector.cassandra;

import org.apache.flink.connector.testframe.TestResource;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.CassandraQueryWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.InetSocketAddress;

/**
 * Junit test environment that contains everything needed at the test suite level: testContainer
 * setup, keyspace setup, Cassandra cluster/session management ClusterBuilder setup).
 */
@Testcontainers
public class CassandraTestEnvironment implements TestResource {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraTestEnvironment.class);
    private static final String DOCKER_CASSANDRA_IMAGE = "cassandra:4.0.8";
    private static final int CQL_PORT = 9042;

    private static final int READ_TIMEOUT_MILLIS = 36000;

    // flushing mem table to SS tables is an asynchronous operation that may take a while
    private static final long FLUSH_MEMTABLES_DELAY = 30_000L;

    public static final String KEYSPACE = "flink";

    private static final String CREATE_KEYSPACE_QUERY =
            "CREATE KEYSPACE "
                    + KEYSPACE
                    + " WITH replication= {'class':'SimpleStrategy', 'replication_factor':1};";

    public static final String SPLITS_TABLE = "flinksplits";
    private static final String CREATE_SPLITS_TABLE_QUERY =
            "CREATE TABLE " + KEYSPACE + "." + SPLITS_TABLE + " (id int PRIMARY KEY, counter int);";
    private static final String INSERT_INTO_FLINK_SPLITS =
            "INSERT INTO " + KEYSPACE + "." + SPLITS_TABLE + " (id, counter)" + " VALUES (%d, %d)";
    private static final int NB_SPLITS_RECORDS = 1000;

    @Container private final CassandraContainer cassandraContainer;

    boolean insertTestDataForSplitSizeTests;
    private Cluster cluster;
    private Session session;
    private ClusterBuilder builderForReading;
    private ClusterBuilder builderForWriting;

    public CassandraTestEnvironment(boolean insertTestDataForSplitSizeTests) {
        this.insertTestDataForSplitSizeTests = insertTestDataForSplitSizeTests;
        cassandraContainer = new CassandraContainer(DOCKER_CASSANDRA_IMAGE);
        // more generous timeouts
        addJavaOpts(
                cassandraContainer,
                "-Dcassandra.request_timeout_in_ms=30000",
                "-Dcassandra.read_request_timeout_in_ms=15000",
                "-Dcassandra.write_request_timeout_in_ms=6000");
    }

    @Override
    public void startUp() throws Exception {
        startEnv();
    }

    @Override
    public void tearDown() throws Exception {
        stopEnv();
    }

    private static void addJavaOpts(GenericContainer<?> container, String... opts) {
        String jvmOpts = container.getEnvMap().getOrDefault("JVM_OPTS", "");
        container.withEnv("JVM_OPTS", jvmOpts + " " + StringUtils.join(opts, " "));
    }

    private void startEnv() throws Exception {
        // configure container start to wait until cassandra is ready to receive queries
        cassandraContainer.waitingFor(new CassandraQueryWaitStrategy());
        // start with retrials
        cassandraContainer.start();
        cassandraContainer.followOutput(
                new Slf4jLogConsumer(LOG),
                OutputFrame.OutputType.END,
                OutputFrame.OutputType.STDERR,
                OutputFrame.OutputType.STDOUT);

        cluster = cassandraContainer.getCluster();
        // ConsistencyLevel.ONE is the minimum level for reading
        builderForReading =
                createBuilderWithConsistencyLevel(
                        ConsistencyLevel.ONE,
                        cassandraContainer.getHost(),
                        cassandraContainer.getMappedPort(CQL_PORT));

        // Lower consistency level ANY is only available for writing.
        builderForWriting =
                createBuilderWithConsistencyLevel(
                        ConsistencyLevel.ANY,
                        cassandraContainer.getHost(),
                        cassandraContainer.getMappedPort(CQL_PORT));
        session = cluster.connect();
        executeRequestWithTimeout(CREATE_KEYSPACE_QUERY);
        // create a dedicated table for split size tests (to avoid having to flush with each test)
        if (insertTestDataForSplitSizeTests) {
            insertTestDataForSplitSizeTests();
        }
    }

    private void insertTestDataForSplitSizeTests() throws Exception {
        executeRequestWithTimeout(CREATE_SPLITS_TABLE_QUERY);
        for (int i = 0; i < NB_SPLITS_RECORDS; i++) {
            executeRequestWithTimeout(String.format(INSERT_INTO_FLINK_SPLITS, i, i));
        }
        flushMemTables(SPLITS_TABLE);
    }

    private void stopEnv() {

        if (session != null) {
            session.close();
        }
        if (cluster != null) {
            cluster.close();
        }
        cassandraContainer.stop();
    }

    private ClusterBuilder createBuilderWithConsistencyLevel(
            ConsistencyLevel consistencyLevel, String host, int port) {
        return new ClusterBuilder() {
            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPointsWithPorts(new InetSocketAddress(host, port))
                        .withQueryOptions(
                                new QueryOptions()
                                        .setConsistencyLevel(consistencyLevel)
                                        .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL))
                        .withSocketOptions(
                                new SocketOptions()
                                        // default timeout x 3
                                        .setConnectTimeoutMillis(15000)
                                        // default timeout x3 and higher than
                                        // request_timeout_in_ms at the cluster level
                                        .setReadTimeoutMillis(READ_TIMEOUT_MILLIS))
                        .build();
            }
        };
    }

    /**
     * Force the flush of cassandra memTables to SSTables in order to update size_estimates. It is
     * needed for the tests because we just inserted records, we need to force cassandra to update
     * size_estimates system table.
     */
    void flushMemTables(String table) throws Exception {
        cassandraContainer.execInContainer("nodetool", "flush", KEYSPACE, table);
        Thread.sleep(FLUSH_MEMTABLES_DELAY);
    }

    public ResultSet executeRequestWithTimeout(String query) {
        return session.execute(
                new SimpleStatement(query).setReadTimeoutMillis(READ_TIMEOUT_MILLIS));
    }

    public ClusterBuilder getBuilderForReading() {
        return builderForReading;
    }

    public ClusterBuilder getBuilderForWriting() {
        return builderForWriting;
    }

    public Session getSession() {
        return session;
    }
}
