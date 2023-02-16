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

import org.apache.flink.connector.testframe.TestResource;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.apache.cassandra.service.StorageServiceMBean;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.rmi.server.RMISocketFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * Junit test environment that contains everything needed at the test suite level: testContainer
 * setup, keyspace setup, Cassandra cluster/session management ClusterBuilder setup).
 */
@Testcontainers
public class CassandraTestEnvironment implements TestResource {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraTestEnvironment.class);
    private static final int CQL_PORT = 9042;

    private static final int MAX_CONNECTION_RETRY = 3;
    private static final long CONNECTION_RETRY_DELAY = 500L;
    private static final int READ_TIMEOUT_MILLIS = 36000;

    private static final String JMX_URL = "service:jmx:rmi://%s/jndi/rmi://%s:%d/jmxrmi";
    private static final String STORAGE_SERVICE_MBEAN =
            "org.apache.cassandra.db:type=StorageService";
    private static final int JMX_PORT = 7199;
    private static final long FLUSH_MEMTABLES_DELAY =
            30_000L; // updating flushing mem table to SS tables is long, it is the minimum delay.
    private static final String JMX_USERNAME = "cassandra";
    private static final String JMX_PASSWORD = "cassandra";

    static final String KEYSPACE = "flink";

    private static final String CREATE_KEYSPACE_QUERY =
            "CREATE KEYSPACE "
                    + KEYSPACE
                    + " WITH replication= {'class':'SimpleStrategy', 'replication_factor':1};";

    // official Cassandra image deactivates jmx, to enable it we need to provide authentication and
    // modify cassandra-env.sh so we created our own image
    private static final String DOCKER_CASSANDRA_IMAGE_WITH_JMX = "echauchot/cassandra-jmx:4.0.8";

    static final String SPLITS_TABLE = "flinksplits";
    private static final String CREATE_SPLITS_TABLE_QUERY =
            "CREATE TABLE " + KEYSPACE + "." + SPLITS_TABLE + " (id int PRIMARY KEY, counter int);";
    private static final String INSERT_INTO_FLINK_SPLITS =
            "INSERT INTO " + KEYSPACE + "." + SPLITS_TABLE + " (id, counter)" + " VALUES (%d, %d)";
    private static final int NB_SPLITS_RECORDS = 1000;

    @Container private final CassandraContainer cassandraContainer = createCassandraContainer();

    @TempDir private File tempDir;

    private static Cluster cluster;
    private static Session session;
    private ClusterBuilder clusterBuilder;

    @Override
    public void startUp() throws Exception {
        startEnv();
    }

    @Override
    public void tearDown() throws Exception {
        stopEnv();
    }

    private void startEnv() throws Exception {
        // need to start the container to be able to patch the configuration file inside the
        // container
        // CASSANDRA_CONTAINER#start() already contains retrials
        cassandraContainer.start();
        cassandraContainer.followOutput(
                new Slf4jLogConsumer(LOG),
                OutputFrame.OutputType.END,
                OutputFrame.OutputType.STDERR,
                OutputFrame.OutputType.STDOUT);
        raiseCassandraRequestsTimeouts();
        // restart the container so that the new timeouts are taken into account
        cassandraContainer.stop();
        cassandraContainer.start();
        cluster = cassandraContainer.getCluster();
        clusterBuilder =
                createBuilderWithConsistencyLevel(
                        ConsistencyLevel.ONE,
                        cassandraContainer.getHost(),
                        cassandraContainer.getMappedPort(CQL_PORT));

        int retried = 0;
        while (retried < MAX_CONNECTION_RETRY) {
            try {
                session = cluster.connect();
                break;
            } catch (NoHostAvailableException e) {
                retried++;
                LOG.debug(
                        "Connection failed with NoHostAvailableException : retry number {}, will retry to connect within {} ms",
                        retried,
                        CONNECTION_RETRY_DELAY);
                if (retried == MAX_CONNECTION_RETRY) {
                    throw new RuntimeException(
                            String.format(
                                    "Failed to connect to Cassandra cluster after %d retries every %d ms",
                                    retried, CONNECTION_RETRY_DELAY),
                            e);
                }
                try {
                    Thread.sleep(CONNECTION_RETRY_DELAY);
                } catch (InterruptedException ignored) {
                }
            }
        }
        session.execute(requestWithTimeout(CREATE_KEYSPACE_QUERY));
        // create a dedicated table for split size tests (to avoid having to flush with each test)
        insertTestDataForSplitSizeTests();
    }

    private void insertTestDataForSplitSizeTests() throws Exception {
        session.execute(requestWithTimeout(CREATE_SPLITS_TABLE_QUERY));
        for (int i = 0; i < NB_SPLITS_RECORDS; i++) {
            session.execute(requestWithTimeout(String.format(INSERT_INTO_FLINK_SPLITS, i, i)));
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
                        .withoutJMXReporting()
                        .withoutMetrics()
                        .build();
            }
        };
    }

    public static CassandraContainer createCassandraContainer() {
        CassandraContainer cassandra =
                new CassandraContainer(
                        DockerImageName.parse(DOCKER_CASSANDRA_IMAGE_WITH_JMX)
                                .asCompatibleSubstituteFor("cassandra"));
        cassandra.addExposedPort(JMX_PORT);
        cassandra.withJmxReporting(true);
        return cassandra;
    }

    private void raiseCassandraRequestsTimeouts() {
        final File tempConfiguration = new File(tempDir, "configuration");
        try {
            cassandraContainer.copyFileFromContainer(
                    "/etc/cassandra/cassandra.yaml", tempConfiguration.getAbsolutePath());
            String configuration =
                    new String(
                            Files.readAllBytes(tempConfiguration.toPath()), StandardCharsets.UTF_8);
            String patchedConfiguration =
                    configuration
                            .replaceAll(
                                    "request_timeout_in_ms: [0-9]+",
                                    "request_timeout_in_ms: 30000") // x3 default timeout
                            .replaceAll(
                                    "read_request_timeout_in_ms: [0-9]+",
                                    "read_request_timeout_in_ms: 15000") // x3 default timeout
                            .replaceAll(
                                    "write_request_timeout_in_ms: [0-9]+",
                                    "write_request_timeout_in_ms: 6000"); // x3 default timeout
            cassandraContainer.copyFileToContainer(
                    Transferable.of(patchedConfiguration.getBytes(StandardCharsets.UTF_8)),
                    "/etc/cassandra/cassandra.yaml");
        } catch (IOException e) {
            throw new RuntimeException("Unable to open Cassandra configuration file ", e);
        } finally {
            tempConfiguration.delete();
        }
    }

    /**
     * Force the flush of cassandra memTables to SSTables in order to update size_estimates. This
     * flush method is what official Cassandra NoteTool does. It is needed for the tests because we
     * just inserted records, we need to force cassandra to update size_estimates system table.
     */
    void flushMemTables(String table) throws Exception {
        final String host = cassandraContainer.getHost();
        final int port = cassandraContainer.getMappedPort(JMX_PORT);
        JMXServiceURL url = new JMXServiceURL(String.format(JMX_URL, host, host, port));
        Map<String, Object> env = new HashMap<>();
        String[] creds = {JMX_USERNAME, JMX_PASSWORD};
        env.put(JMXConnector.CREDENTIALS, creds);
        env.put(
                "com.sun.jndi.rmi.factory.socket",
                RMISocketFactory.getDefaultSocketFactory()); // connection without ssl
        JMXConnector jmxConnector = JMXConnectorFactory.connect(url, env);
        MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
        ObjectName objectName = new ObjectName(STORAGE_SERVICE_MBEAN);
        StorageServiceMBean mBeanProxy =
                JMX.newMBeanProxy(mBeanServerConnection, objectName, StorageServiceMBean.class);
        mBeanProxy.forceKeyspaceFlush(KEYSPACE, table);
        jmxConnector.close();
        Thread.sleep(FLUSH_MEMTABLES_DELAY);
    }

    static Statement requestWithTimeout(String query) {
        return new SimpleStatement(query).setReadTimeoutMillis(READ_TIMEOUT_MILLIS);
    }

    public ClusterBuilder getClusterBuilder() {
        return clusterBuilder;
    }

    public Session getSession() {
        return session;
    }
}
