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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link CassandraClusterBuilder}. */
class CassandraClusterBuilderTest {

    /** Tests default constructor creates builder with sensible defaults for local development. */
    @Test
    void testDefaultConstructor() {
        CassandraClusterBuilder builder = new CassandraClusterBuilder();

        /** Should build cluster without throwing exceptions */
        Cluster cluster = builder.getCluster();
        assertThat(cluster).isNotNull();

        /** Verify default configuration is accessible */
        assertThat(cluster.getConfiguration().getProtocolOptions().getPort()).isEqualTo(9042);
        assertThat(cluster.getConfiguration().getQueryOptions().getConsistencyLevel())
                .isEqualTo(ConsistencyLevel.LOCAL_ONE);

        cluster.close();
    }

    /** Tests parameterized constructor with valid configuration values. */
    @Test
    void testParameterizedConstructor() {
        CassandraClusterBuilder builder =
                new CassandraClusterBuilder(
                        "192.168.1.100", 9043, "test_user", "test_pass", "QUORUM", 10000L, 15000L);

        Cluster cluster = builder.getCluster();
        assertThat(cluster).isNotNull();
        assertThat(cluster.getConfiguration().getProtocolOptions().getPort()).isEqualTo(9043);
        assertThat(cluster.getConfiguration().getQueryOptions().getConsistencyLevel())
                .isEqualTo(ConsistencyLevel.QUORUM);

        cluster.close();
    }

    /** Tests valid consistency level parsing for all supported levels. */
    @Test
    void testValidConsistencyLevels() {
        for (ConsistencyLevel level : ConsistencyLevel.values()) {
            CassandraClusterBuilder builder =
                    new CassandraClusterBuilder(
                            "localhost",
                            9042,
                            "cassandra",
                            "cassandra",
                            level.name(),
                            5000L,
                            12000L);

            Cluster cluster = builder.getCluster();
            assertThat(cluster.getConfiguration().getQueryOptions().getConsistencyLevel())
                    .isEqualTo(level);

            cluster.close();
        }
    }

    /** Tests consistency level parsing is case insensitive. */
    @Test
    void testConsistencyLevelCaseInsensitive() {
        CassandraClusterBuilder builder =
                new CassandraClusterBuilder(
                        "localhost", 9042, "cassandra", "cassandra", "local_one", 5000L, 12000L);

        Cluster cluster = builder.getCluster();
        assertThat(cluster.getConfiguration().getQueryOptions().getConsistencyLevel())
                .isEqualTo(ConsistencyLevel.LOCAL_ONE);

        cluster.close();
    }

    /** Tests error handling for null consistency level. */
    @Test
    void testNullConsistencyLevelThrowsException() {
        assertThatThrownBy(
                        () ->
                                new CassandraClusterBuilder(
                                        "localhost",
                                        9042,
                                        "cassandra",
                                        "cassandra",
                                        null,
                                        5000L,
                                        12000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Consistency level cannot be empty");
    }

    /** Tests error handling for empty consistency level. */
    @Test
    void testEmptyConsistencyLevelThrowsException() {
        assertThatThrownBy(
                        () ->
                                new CassandraClusterBuilder(
                                        "localhost",
                                        9042,
                                        "cassandra",
                                        "cassandra",
                                        "",
                                        5000L,
                                        12000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Consistency level cannot be empty");
    }

    /** Tests error handling for invalid consistency level. */
    @Test
    void testInvalidConsistencyLevelThrowsException() {
        assertThatThrownBy(
                        () ->
                                new CassandraClusterBuilder(
                                        "localhost",
                                        9042,
                                        "cassandra",
                                        "cassandra",
                                        "INVALID_LEVEL",
                                        5000L,
                                        12000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid consistency level: INVALID_LEVEL")
                .hasMessageContaining("Valid values are:");
    }

    /** Tests maximum valid timeout values. */
    @Test
    void testMaximumValidTimeouts() {
        int maxTimeout = Integer.MAX_VALUE;
        CassandraClusterBuilder builder =
                new CassandraClusterBuilder(
                        "localhost",
                        9042,
                        "cassandra",
                        "cassandra",
                        "LOCAL_ONE",
                        maxTimeout,
                        maxTimeout);

        Cluster cluster = builder.getCluster();
        assertThat(cluster).isNotNull();
        assertThat(cluster.getConfiguration().getSocketOptions().getConnectTimeoutMillis())
                .isEqualTo(maxTimeout);
        assertThat(cluster.getConfiguration().getSocketOptions().getReadTimeoutMillis())
                .isEqualTo(maxTimeout);

        cluster.close();
    }

    /** Tests cluster configuration with custom socket options. */
    @Test
    void testCustomSocketOptions() {
        CassandraClusterBuilder builder =
                new CassandraClusterBuilder(
                        "localhost", 9042, "cassandra", "cassandra", "LOCAL_ONE", 8000L, 20000L);

        Cluster cluster = builder.getCluster();
        assertThat(cluster.getConfiguration().getSocketOptions().getConnectTimeoutMillis())
                .isEqualTo(8000);
        assertThat(cluster.getConfiguration().getSocketOptions().getReadTimeoutMillis())
                .isEqualTo(20000);

        cluster.close();
    }

    /** Tests cluster configuration with custom credentials. */
    @Test
    void testCustomCredentials() {
        CassandraClusterBuilder builder =
                new CassandraClusterBuilder(
                        "localhost",
                        9042,
                        "custom_user",
                        "custom_pass",
                        "LOCAL_ONE",
                        5000L,
                        12000L);

        /**
         * Should build cluster without authentication errors (actual auth tested in integration
         * tests)
         */
        Cluster cluster = builder.getCluster();
        assertThat(cluster).isNotNull();

        cluster.close();
    }
}
