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

import org.apache.flink.connector.cassandra.source.utils.QueryValidator;
import org.apache.flink.connector.testframe.TestResource;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * JUnit test environment for ScyllaDB using testContainers. Provides a 2-node ScyllaDB cluster for
 * integration testing, validating ScyllaDB as a drop-in replacement for Apache Cassandra in the
 * Flink Cassandra Connector.
 *
 * <p>This class mirrors CassandraTestEnvironment but uses ScyllaDB containers with
 * ScyllaDB-specific configuration (e.g., --reactor-backend=epoll for macOS compatibility).
 */
@Testcontainers
public class ScyllaDBTestEnvironment implements TestResource {
    private static final Logger LOG = LoggerFactory.getLogger(ScyllaDBTestEnvironment.class);

    /** ScyllaDB Docker image - using latest open source version. */
    public static final String DOCKER_SCYLLADB_IMAGE = "scylladb/scylla:2025.1.4";

    private static final int CQL_PORT = 9042;
    private static final int READ_TIMEOUT_MILLIS = 36000;

    /** Increased timeout for ScyllaDB container startup - may need more time than Cassandra. */
    private static final int STARTUP_TIMEOUT_MINUTES = 5;

    public static final String KEYSPACE = "flink";

    private static final String CREATE_KEYSPACE_QUERY =
            "CREATE KEYSPACE "
                    + KEYSPACE
                    + " WITH replication= {'class':'SimpleStrategy', 'replication_factor':2};";

    public static final String SPLITS_TABLE = "flinksplits";
    /*
         CREATE TABLE flink.flinksplits (col1 int, col2 int, col3 int, col4 int, PRIMARY KEY ((col1, col2), col3));
         - partition key is (col1, col2)
         - primary key is ((col1, col2), col3) so col3 is a clustering column
         - col4 is a regular column
    */
    private static final String CREATE_SPLITS_TABLE_QUERY =
            "CREATE TABLE "
                    + KEYSPACE
                    + "."
                    + SPLITS_TABLE
                    + " (col1 int, col2 int, col3 int, col4 int, PRIMARY KEY ((col1, col2), col3));";
    private static final String CREATE_INDEX =
            "CREATE INDEX col4index ON " + KEYSPACE + "." + SPLITS_TABLE + " (col4);";
    private static final String INSERT_INTO_FLINK_SPLITS =
            "INSERT INTO "
                    + KEYSPACE
                    + "."
                    + SPLITS_TABLE
                    + " (col1, col2, col3, col4)"
                    + " VALUES (%d, %d, %d, %d)";
    private static final int NB_SPLITS_RECORDS = 1000;

    @Container private final GenericContainer<?> scyllaContainer1;
    @Container private final GenericContainer<?> scyllaContainer2;

    boolean insertTestDataForSplitSizeTests;
    private Cluster cluster;
    private Session session;
    private ClusterBuilder builderForReading;
    private ClusterBuilder builderForWriting;
    private QueryValidator queryValidator;

    public ScyllaDBTestEnvironment(boolean insertTestDataForSplitSizeTests) {
        this.insertTestDataForSplitSizeTests = insertTestDataForSplitSizeTests;

        Network network = Network.newNetwork();

        // ScyllaDB container 1 - seed node
        scyllaContainer1 =
                new GenericContainer<>(DOCKER_SCYLLADB_IMAGE)
                        .withNetwork(network)
                        .withNetworkAliases("scylla")
                        .withExposedPorts(CQL_PORT)
                        // ScyllaDB-specific configuration:
                        // --reactor-backend=epoll: For macOS compatibility with multi-node clusters
                        // --smp 1: Limit to single CPU core for test environments
                        .withCommand("--reactor-backend=epoll", "--smp", "1")
                        .waitingFor(
                                Wait.forLogMessage(".*Starting listening for CQL clients.*", 1)
                                        .withStartupTimeout(
                                                Duration.ofMinutes(STARTUP_TIMEOUT_MINUTES)));

        // ScyllaDB container 2 - joins the cluster via seeds
        scyllaContainer2 =
                new GenericContainer<>(DOCKER_SCYLLADB_IMAGE)
                        .withNetwork(network)
                        .withExposedPorts(CQL_PORT)
                        .withCommand(
                                "--reactor-backend=epoll",
                                "--smp",
                                "1",
                                "--seeds=scylla") // Connect to first node as seed
                        .waitingFor(
                                Wait.forLogMessage(".*Starting listening for CQL clients.*", 1)
                                        .withStartupTimeout(
                                                Duration.ofMinutes(STARTUP_TIMEOUT_MINUTES)));
    }

    @Override
    public void startUp() throws Exception {
        startEnv();
    }

    @Override
    public void tearDown() throws Exception {
        stopEnv();
    }

    private void startEnv() throws Exception {
        // Start containers sequentially to ensure proper cluster formation
        LOG.info("Starting ScyllaDB container 1 (seed node)...");
        scyllaContainer1.start();
        scyllaContainer1.followOutput(
                new Slf4jLogConsumer(LOG),
                OutputFrame.OutputType.END,
                OutputFrame.OutputType.STDERR,
                OutputFrame.OutputType.STDOUT);

        // Wait a bit for the first node to be fully ready before starting second
        Thread.sleep(5000);

        LOG.info("Starting ScyllaDB container 2...");
        scyllaContainer2.start();
        scyllaContainer2.followOutput(
                new Slf4jLogConsumer(LOG),
                OutputFrame.OutputType.END,
                OutputFrame.OutputType.STDERR,
                OutputFrame.OutputType.STDOUT);

        // Wait for cluster to stabilize
        Thread.sleep(5000);
        LOG.info("ScyllaDB cluster started successfully");

        // Build cluster connection using DataStax driver
        String host = scyllaContainer1.getHost();
        Integer port = scyllaContainer1.getMappedPort(CQL_PORT);

        cluster =
                Cluster.builder()
                        .addContactPointsWithPorts(new InetSocketAddress(host, port))
                        .withQueryOptions(
                                new QueryOptions()
                                        .setConsistencyLevel(ConsistencyLevel.ONE)
                                        .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL))
                        .withSocketOptions(
                                new SocketOptions()
                                        .setConnectTimeoutMillis(15000)
                                        .setReadTimeoutMillis(READ_TIMEOUT_MILLIS))
                        .build();

        // ConsistencyLevel.ONE is the minimum level for reading
        builderForReading = createBuilderWithConsistencyLevel(ConsistencyLevel.ONE, host, port);
        queryValidator = new QueryValidator(builderForReading);
        builderForWriting = createBuilderWithConsistencyLevel(ConsistencyLevel.ONE, host, port);

        session = cluster.connect();
        executeRequestWithTimeout(CREATE_KEYSPACE_QUERY);

        // Create a dedicated table for split size tests
        if (insertTestDataForSplitSizeTests) {
            insertTestDataForSplitSizeTests();
        }
    }

    private void insertTestDataForSplitSizeTests() throws Exception {
        executeRequestWithTimeout(CREATE_SPLITS_TABLE_QUERY);
        executeRequestWithTimeout(CREATE_INDEX);
        for (int i = 0; i < NB_SPLITS_RECORDS; i++) {
            executeRequestWithTimeout(String.format(INSERT_INTO_FLINK_SPLITS, i, i, i, i));
        }
        refreshSizeEstimates(SPLITS_TABLE);
    }

    private void stopEnv() {
        if (session != null) {
            session.close();
        }
        if (cluster != null) {
            cluster.close();
        }
        try {
            scyllaContainer1.stop();
        } catch (Exception e) {
            // Do not fail the test for a stop failure and allow the other container to stop
            LOG.error("ScyllaDB test container 1 failed to stop.", e);
        }
        try {
            scyllaContainer2.stop();
        } catch (Exception e) {
            // Do not fail the test for a stop failure
            LOG.error("ScyllaDB test container 2 failed to stop.", e);
        }
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
     * Force the refresh of system.size_estimates table. It is needed for the tests because we just
     * inserted records. It is done on a single node as the size estimation for split generation is
     * evaluated based on the ring fraction the connect node represents in the cluster. We first
     * flush the MemTables to SSTables because the size estimates are only on SSTables. Then we
     * refresh the size estimates.
     *
     * <p>Note: ScyllaDB uses the same nodetool commands as Cassandra for these operations.
     */
    void refreshSizeEstimates(String table) throws Exception {
        final ExecResult execResult1 =
                scyllaContainer1.execInContainer("nodetool", "flush", KEYSPACE, table);
        LOG.info(
                "nodetool flush exit code: {}, stdout: {}, stderr: {}",
                execResult1.getExitCode(),
                execResult1.getStdout(),
                execResult1.getStderr());

        final ExecResult execResult2 =
                scyllaContainer1.execInContainer("nodetool", "refreshsizeestimates");
        LOG.info(
                "nodetool refreshsizeestimates exit code: {}, stdout: {}, stderr: {}",
                execResult2.getExitCode(),
                execResult2.getStdout(),
                execResult2.getStderr());

        if (execResult1.getExitCode() != 0) {
            throw new RuntimeException(
                    String.format(
                            "Failed to flush on ScyllaDB cluster. Exit code: %d, stderr: %s",
                            execResult1.getExitCode(), execResult1.getStderr()));
        }
        if (execResult2.getExitCode() != 0) {
            LOG.warn(
                    "refreshsizeestimates returned non-zero exit code: {}. This may be expected for ScyllaDB. Continuing...",
                    execResult2.getExitCode());
            // ScyllaDB may not support refreshsizeestimates command or may handle it differently
            // We'll continue and wait for size estimates to be populated naturally
        }
        List<Row> partitions = new ArrayList<>();
        while (partitions.isEmpty()
                || partitions.stream().anyMatch(row -> row.getLong("mean_partition_size") == 0L)) {
            Thread.sleep(1000);
            partitions =
                    session.execute(
                                    "SELECT range_start, range_end, partitions_count, mean_partition_size FROM "
                                            + "system.size_estimates WHERE keyspace_name = ? AND table_name = ?",
                                    KEYSPACE,
                                    table)
                            .all();
        }
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

    public QueryValidator getQueryValidator() {
        return queryValidator;
    }

    public Session getSession() {
        return session;
    }

    public String getContactPoint() {
        return scyllaContainer1.getHost();
    }

    public int getPort() {
        return scyllaContainer1.getMappedPort(CQL_PORT);
    }

    public String getUsername() {
        // ScyllaDB uses same default credentials as Cassandra
        return "cassandra";
    }

    public String getPassword() {
        // ScyllaDB uses same default credentials as Cassandra
        return "cassandra";
    }
}
