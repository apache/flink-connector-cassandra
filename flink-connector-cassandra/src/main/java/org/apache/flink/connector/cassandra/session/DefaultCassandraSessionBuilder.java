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

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.cassandra.CassandraSessionBuilder;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link CassandraSessionBuilder} creating sessions from contact points and optional settings.
 *
 * <p>When no local datacenter is set, the session uses the {@code DcInferringLoadBalancingPolicy}
 * of the driver, which infers the local datacenter from the contact points. Driver options are
 * passed as they are to the driver configuration and take precedence over the other settings.
 *
 * <p>When a secure connect bundle is set, the session connects to the cloud database described by
 * the bundle, the contact points and the local datacenter are ignored.
 */
@Internal
public class DefaultCassandraSessionBuilder extends CassandraSessionBuilder {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DefaultCassandraSessionBuilder.class);

    private static final Pattern HOST_PORT_PATTERN = Pattern.compile("^([^:]+)(?::(\\d+))?$");
    private static final String DC_INFERRING_LOAD_BALANCING_POLICY =
            "DcInferringLoadBalancingPolicy";

    private final String hosts;
    private final int port;
    @Nullable private final String localDatacenter;
    @Nullable private final String secureConnectBundle;
    @Nullable private final String username;
    @Nullable private final String password;
    @Nullable private final DefaultConsistencyLevel consistencyLevel;
    @Nullable private final Long connectTimeoutMs;
    @Nullable private final Long requestTimeoutMs;
    private final Map<String, String> driverOptions;

    private DefaultCassandraSessionBuilder(Builder builder) {
        this.hosts = builder.hosts;
        this.port = builder.port;
        this.localDatacenter = builder.localDatacenter;
        this.secureConnectBundle =
                builder.secureConnectBundle == null || builder.secureConnectBundle.trim().isEmpty()
                        ? null
                        : builder.secureConnectBundle.trim();
        this.username = builder.username;
        this.password = builder.password;
        this.consistencyLevel =
                builder.consistencyLevel == null
                        ? null
                        : parseConsistencyLevel(builder.consistencyLevel);
        this.connectTimeoutMs =
                builder.connectTimeoutMs == null
                        ? null
                        : validateTimeout(builder.connectTimeoutMs, "connect");
        this.requestTimeoutMs =
                builder.requestTimeoutMs == null
                        ? null
                        : validateTimeout(builder.requestTimeoutMs, "read");
        this.driverOptions = new HashMap<>(builder.driverOptions);
        if (secureConnectBundle == null) {
            // fail fast on invalid hosts
            parseContactPoints();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    protected CqlSession buildSession(CqlSessionBuilder builder) {
        builder.withConfigLoader(createConfigLoader());
        if (secureConnectBundle != null) {
            // the bundle contains the contact points and the local datacenter
            LOG.info(
                    "A secure connect bundle is provided, the hosts and the local datacenter are ignored.");
        } else {
            builder.addContactPoints(parseContactPoints());
            if (localDatacenter != null) {
                builder.withLocalDatacenter(localDatacenter);
            }
        }
        if (username != null) {
            builder.withAuthCredentials(username, password);
        }
        return builder.build();
    }

    /** Creates the driver configuration of the session, package-private for testing. */
    DriverConfigLoader createConfigLoader() {
        final ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder =
                DriverConfigLoader.programmaticBuilder();
        if (secureConnectBundle != null) {
            // the driver accepts a path of the local filesystem or a URL
            configLoaderBuilder.withString(
                    DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, secureConnectBundle);
        } else if (localDatacenter == null) {
            configLoaderBuilder.withString(
                    DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                    DC_INFERRING_LOAD_BALANCING_POLICY);
        }
        if (consistencyLevel != null) {
            configLoaderBuilder.withString(
                    DefaultDriverOption.REQUEST_CONSISTENCY, consistencyLevel.name());
        }
        if (connectTimeoutMs != null) {
            configLoaderBuilder.withDuration(
                    DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT,
                    Duration.ofMillis(connectTimeoutMs));
        }
        if (requestTimeoutMs != null) {
            configLoaderBuilder.withDuration(
                    DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(requestTimeoutMs));
        }
        driverOptions.forEach((path, value) -> configLoaderBuilder.withString(() -> path, value));
        return configLoaderBuilder.build();
    }

    /** Parses the hosts into contact points, package-private for testing. */
    List<InetSocketAddress> parseContactPoints() {
        if (hosts == null || hosts.trim().isEmpty()) {
            throw new IllegalArgumentException("Hosts configuration cannot be empty");
        }

        final List<InetSocketAddress> contactPoints = new ArrayList<>();
        for (String hostEntry : hosts.split(",")) {
            String trimmedEntry = hostEntry.trim();
            if (trimmedEntry.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format("Empty host entry found in: %s", hosts));
            }

            Matcher matcher = HOST_PORT_PATTERN.matcher(trimmedEntry);
            if (!matcher.matches()) {
                throw new IllegalArgumentException(
                        String.format("Invalid host format: %s", trimmedEntry));
            }

            String host = matcher.group(1);
            String portGroup = matcher.group(2);

            int hostPort = port;
            if (portGroup != null) {
                // Host with specific port
                hostPort = Integer.parseInt(portGroup);
                if (hostPort <= 0 || hostPort > 65535) {
                    throw new IllegalArgumentException(
                            String.format("Invalid port number: %s for host: %s", hostPort, host));
                }
            }
            // the driver resolves the address when the session is created
            contactPoints.add(InetSocketAddress.createUnresolved(host, hostPort));
        }
        return contactPoints;
    }

    private static long validateTimeout(long timeoutMs, String timeoutType) {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s timeout must be positive, got: %d ms", timeoutType, timeoutMs));
        }
        if (timeoutMs > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s timeout too large: %d ms (max: %d ms)",
                            timeoutType, timeoutMs, Integer.MAX_VALUE));
        }
        return timeoutMs;
    }

    private static DefaultConsistencyLevel parseConsistencyLevel(String level) {
        if (level.trim().isEmpty()) {
            throw new IllegalArgumentException("Consistency level cannot be empty");
        }

        try {
            return DefaultConsistencyLevel.valueOf(level.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid consistency level: "
                            + level
                            + ". Valid values are: "
                            + Arrays.toString(DefaultConsistencyLevel.values()),
                    e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultCassandraSessionBuilder that = (DefaultCassandraSessionBuilder) o;
        return port == that.port
                && Objects.equals(hosts, that.hosts)
                && Objects.equals(localDatacenter, that.localDatacenter)
                && Objects.equals(secureConnectBundle, that.secureConnectBundle)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && consistencyLevel == that.consistencyLevel
                && Objects.equals(connectTimeoutMs, that.connectTimeoutMs)
                && Objects.equals(requestTimeoutMs, that.requestTimeoutMs)
                && Objects.equals(driverOptions, that.driverOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                hosts,
                port,
                localDatacenter,
                secureConnectBundle,
                username,
                password,
                consistencyLevel,
                connectTimeoutMs,
                requestTimeoutMs,
                driverOptions);
    }

    /** Builder of {@link DefaultCassandraSessionBuilder}. */
    public static final class Builder {

        private String hosts = "127.0.0.1";
        private int port = 9042;
        @Nullable private String localDatacenter;
        @Nullable private String secureConnectBundle;
        @Nullable private String username;
        @Nullable private String password;
        @Nullable private String consistencyLevel;
        @Nullable private Long connectTimeoutMs;
        @Nullable private Long requestTimeoutMs;
        private Map<String, String> driverOptions = Collections.emptyMap();

        private Builder() {}

        /**
         * Sets the contact points as a comma separated list of {@code host} or {@code host:port}.
         */
        public Builder setHosts(String hosts) {
            this.hosts = hosts;
            return this;
        }

        /** Sets the port of the contact points that do not define their own port. */
        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setLocalDatacenter(@Nullable String localDatacenter) {
            this.localDatacenter = localDatacenter;
            return this;
        }

        /**
         * Sets the secure connect bundle of a cloud database, as a path of the local filesystem or
         * a URL. The bundle must be accessible from all the nodes of the Flink cluster. When it is
         * set, the hosts, the port and the local datacenter are ignored.
         */
        public Builder setSecureConnectBundle(@Nullable String secureConnectBundle) {
            this.secureConnectBundle = secureConnectBundle;
            return this;
        }

        public Builder setCredentials(@Nullable String username, @Nullable String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        public Builder setConsistencyLevel(@Nullable String consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        public Builder setConnectTimeoutMs(@Nullable Long connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
            return this;
        }

        /** Sets the timeout of a request, it includes the retries of the request. */
        public Builder setRequestTimeoutMs(@Nullable Long requestTimeoutMs) {
            this.requestTimeoutMs = requestTimeoutMs;
            return this;
        }

        /**
         * Sets driver options, the keys are paths of the driver configuration without the {@code
         * datastax-java-driver} prefix, for example {@code basic.request.page-size}.
         */
        public Builder setDriverOptions(Map<String, String> driverOptions) {
            this.driverOptions = driverOptions;
            return this;
        }

        public DefaultCassandraSessionBuilder build() {
            return new DefaultCassandraSessionBuilder(this);
        }
    }
}
