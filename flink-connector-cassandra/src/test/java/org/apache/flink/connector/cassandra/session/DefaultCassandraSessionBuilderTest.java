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

package org.apache.flink.connector.cassandra.session;

import org.apache.flink.util.InstantiationUtil;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link DefaultCassandraSessionBuilder}. */
class DefaultCassandraSessionBuilderTest {

    /** Tests that the default builder targets a local node and keeps the driver defaults. */
    @Test
    void testDefaults() {
        DefaultCassandraSessionBuilder builder = DefaultCassandraSessionBuilder.builder().build();

        assertThat(builder.parseContactPoints())
                .containsExactly(InetSocketAddress.createUnresolved("127.0.0.1", 9042));

        try (DriverConfigLoader configLoader = builder.createConfigLoader()) {
            DriverExecutionProfile profile = configLoader.getInitialConfig().getDefaultProfile();
            assertThat(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
                    .isEqualTo(DefaultConsistencyLevel.LOCAL_ONE.name());
            assertThat(profile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
                    .isEqualTo(Duration.ofSeconds(2));
        }
    }

    /** Tests that the settings are applied to the driver configuration. */
    @Test
    void testConfiguredSettings() {
        DefaultCassandraSessionBuilder builder =
                DefaultCassandraSessionBuilder.builder()
                        .setHosts("192.168.1.100")
                        .setPort(9043)
                        .setLocalDatacenter("dc1")
                        .setCredentials("test_user", "test_pass")
                        .setConsistencyLevel("QUORUM")
                        .setConnectTimeoutMs(10000L)
                        .setRequestTimeoutMs(15000L)
                        .build();

        assertThat(builder.parseContactPoints())
                .containsExactly(InetSocketAddress.createUnresolved("192.168.1.100", 9043));

        try (DriverConfigLoader configLoader = builder.createConfigLoader()) {
            DriverExecutionProfile profile = configLoader.getInitialConfig().getDefaultProfile();
            assertThat(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
                    .isEqualTo("QUORUM");
            assertThat(profile.getDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT))
                    .isEqualTo(Duration.ofMillis(10000L));
            assertThat(profile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
                    .isEqualTo(Duration.ofMillis(15000L));
            // the local datacenter is set, the default load balancing policy of the driver is used
            assertThat(profile.getString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS))
                    .isEqualTo("DefaultLoadBalancingPolicy");
        }
    }

    /** Tests that the local datacenter is inferred from the contact points when it is not set. */
    @Test
    void testLocalDatacenterInferredWhenNotSet() {
        DefaultCassandraSessionBuilder builder =
                DefaultCassandraSessionBuilder.builder().setHosts("localhost").build();

        try (DriverConfigLoader configLoader = builder.createConfigLoader()) {
            DriverExecutionProfile profile = configLoader.getInitialConfig().getDefaultProfile();
            assertThat(profile.getString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS))
                    .isEqualTo("DcInferringLoadBalancingPolicy");
        }
    }

    /**
     * Tests that the secure connect bundle is passed to the driver and that the hosts and the local
     * datacenter are ignored.
     */
    @Test
    void testSecureConnectBundle() {
        DefaultCassandraSessionBuilder builder =
                DefaultCassandraSessionBuilder.builder()
                        // invalid hosts are not even parsed
                        .setHosts("host1:port")
                        .setLocalDatacenter("dc1")
                        .setSecureConnectBundle("/path/to/secure-connect-database.zip")
                        .setCredentials("token", "secret")
                        .build();

        try (DriverConfigLoader configLoader = builder.createConfigLoader()) {
            DriverExecutionProfile profile = configLoader.getInitialConfig().getDefaultProfile();
            assertThat(profile.getString(DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE))
                    .isEqualTo("/path/to/secure-connect-database.zip");
            assertThat(profile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
                    .isFalse();
            // the local datacenter of the bundle is used by the default policy of the driver
            assertThat(profile.getString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS))
                    .isEqualTo("DefaultLoadBalancingPolicy");
        }
    }

    /** Tests that connecting with a bundle that does not exist fails with the bundle location. */
    @Test
    void testMissingSecureConnectBundleFailsOnConnection() {
        DefaultCassandraSessionBuilder builder =
                DefaultCassandraSessionBuilder.builder()
                        .setSecureConnectBundle("/path/to/missing-secure-connect-database.zip")
                        .setCredentials("token", "secret")
                        .build();

        assertThatThrownBy(builder::getSession)
                .hasStackTraceContaining("missing-secure-connect-database.zip");
    }

    /** Tests multiple hosts with and without their own port. */
    @Test
    void testMultipleHosts() {
        DefaultCassandraSessionBuilder builder =
                DefaultCassandraSessionBuilder.builder()
                        .setHosts("host1:9043, host2 ,host3:9044")
                        .setPort(9042)
                        .build();

        assertThat(builder.parseContactPoints())
                .containsExactly(
                        InetSocketAddress.createUnresolved("host1", 9043),
                        InetSocketAddress.createUnresolved("host2", 9042),
                        InetSocketAddress.createUnresolved("host3", 9044));
    }

    /** Tests that invalid hosts are rejected when the builder is created. */
    @Test
    void testInvalidHostsThrowException() {
        assertThatThrownBy(() -> DefaultCassandraSessionBuilder.builder().setHosts(" ").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Hosts configuration cannot be empty");

        assertThatThrownBy(
                        () ->
                                DefaultCassandraSessionBuilder.builder()
                                        .setHosts("host1,,host2")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Empty host entry");

        assertThatThrownBy(
                        () ->
                                DefaultCassandraSessionBuilder.builder()
                                        .setHosts("host1:70000")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid port number");

        assertThatThrownBy(
                        () ->
                                DefaultCassandraSessionBuilder.builder()
                                        .setHosts("host1:port")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid host format");
    }

    /** Tests all valid consistency levels can be parsed correctly. */
    @Test
    void testValidConsistencyLevels() {
        for (DefaultConsistencyLevel level : DefaultConsistencyLevel.values()) {
            DefaultCassandraSessionBuilder builder =
                    DefaultCassandraSessionBuilder.builder()
                            .setConsistencyLevel(level.name())
                            .build();

            try (DriverConfigLoader configLoader = builder.createConfigLoader()) {
                assertThat(
                                configLoader
                                        .getInitialConfig()
                                        .getDefaultProfile()
                                        .getString(DefaultDriverOption.REQUEST_CONSISTENCY))
                        .isEqualTo(level.name());
            }
        }
    }

    /** Tests consistency level parsing is case-insensitive. */
    @Test
    void testConsistencyLevelCaseInsensitive() {
        DefaultCassandraSessionBuilder builder =
                DefaultCassandraSessionBuilder.builder().setConsistencyLevel("quorum").build();

        try (DriverConfigLoader configLoader = builder.createConfigLoader()) {
            assertThat(
                            configLoader
                                    .getInitialConfig()
                                    .getDefaultProfile()
                                    .getString(DefaultDriverOption.REQUEST_CONSISTENCY))
                    .isEqualTo("QUORUM");
        }
    }

    /** Tests empty and invalid consistency levels throw descriptive exceptions. */
    @Test
    void testInvalidConsistencyLevelThrowsException() {
        assertThatThrownBy(
                        () ->
                                DefaultCassandraSessionBuilder.builder()
                                        .setConsistencyLevel("  ")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Consistency level cannot be empty");

        assertThatThrownBy(
                        () ->
                                DefaultCassandraSessionBuilder.builder()
                                        .setConsistencyLevel("INVALID_LEVEL")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid consistency level: INVALID_LEVEL")
                .hasMessageContaining("Valid values are:");
    }

    /** Tests invalid timeouts throw descriptive exceptions. */
    @Test
    void testInvalidTimeoutsThrowException() {
        assertThatThrownBy(
                        () ->
                                DefaultCassandraSessionBuilder.builder()
                                        .setConnectTimeoutMs(0L)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("connect timeout must be positive");

        assertThatThrownBy(
                        () ->
                                DefaultCassandraSessionBuilder.builder()
                                        .setRequestTimeoutMs((long) Integer.MAX_VALUE + 1)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("read timeout too large");
    }

    /**
     * Tests that the driver options given as strings are converted by the driver configuration to
     * the type of the option and take precedence over the other settings.
     */
    @Test
    void testDriverOptions() {
        Map<String, String> driverOptions = new HashMap<>();
        driverOptions.put("basic.request.page-size", "1000");
        driverOptions.put("basic.request.default-idempotence", "true");
        driverOptions.put("basic.request.timeout", "7 seconds");
        driverOptions.put("basic.load-balancing-policy.local-datacenter", "dc2");
        driverOptions.put("advanced.request-id.generator.class", "UuidRequestIdGenerator");

        DefaultCassandraSessionBuilder builder =
                DefaultCassandraSessionBuilder.builder()
                        .setRequestTimeoutMs(15000L)
                        .setDriverOptions(driverOptions)
                        .build();

        try (DriverConfigLoader configLoader = builder.createConfigLoader()) {
            DriverExecutionProfile profile = configLoader.getInitialConfig().getDefaultProfile();
            assertThat(profile.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE)).isEqualTo(1000);
            assertThat(profile.getBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE))
                    .isTrue();
            assertThat(profile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
                    .isEqualTo(Duration.ofSeconds(7));
            assertThat(profile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
                    .isEqualTo("dc2");
            assertThat(profile.getString(DefaultDriverOption.REQUEST_ID_GENERATOR_CLASS))
                    .isEqualTo("UuidRequestIdGenerator");
        }
    }

    /** Tests that the builder can be shipped to the task managers. */
    @Test
    void testSerialization() throws Exception {
        Map<String, String> driverOptions = new HashMap<>();
        driverOptions.put("basic.request.page-size", "1000");
        DefaultCassandraSessionBuilder builder =
                DefaultCassandraSessionBuilder.builder()
                        .setHosts("host1:9043")
                        .setLocalDatacenter("dc1")
                        .setCredentials("test_user", "test_pass")
                        .setConsistencyLevel("QUORUM")
                        .setDriverOptions(driverOptions)
                        .build();

        DefaultCassandraSessionBuilder copy =
                InstantiationUtil.clone(builder, getClass().getClassLoader());

        assertThat(copy).isEqualTo(builder).hasSameHashCodeAs(builder);
    }
}
