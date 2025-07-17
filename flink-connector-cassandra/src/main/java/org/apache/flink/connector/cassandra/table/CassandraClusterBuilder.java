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

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Builder for creating Cassandra cluster connections with proper configuration. */
@Internal
public class CassandraClusterBuilder extends ClusterBuilder {

    private static final Pattern HOST_PORT_PATTERN = Pattern.compile("^([^:]+)(?::(\\d+))?$");

    private final String hosts;
    private final int port;
    private final String username;
    private final String password;
    private final ConsistencyLevel consistencyLevel;
    private final long connectTimeoutMs;
    private final long readTimeoutMs;

    /** Default constructor with sensible defaults for local development. */
    public CassandraClusterBuilder() {
        this("127.0.0.1", 9042, "cassandra", "cassandra", "LOCAL_ONE", 5000L, 12000L);
    }

    public CassandraClusterBuilder(
            String hosts,
            int port,
            String username,
            String password,
            String consistencyLevel,
            long connectTimeoutMs,
            long readTimeoutMs) {
        this.hosts = hosts;
        this.port = port;
        this.username = username;
        this.password = password;
        this.consistencyLevel = parseConsistencyLevel(consistencyLevel);
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
    }

    @Override
    protected Cluster buildCluster(Cluster.Builder builder) {
        configureHosts(builder, hosts);

        return builder.withCredentials(username, password)
                .withPort(port)
                .withQueryOptions(createQueryOptions())
                .withSocketOptions(createSocketOptions())
                .build();
    }

    private void configureHosts(Cluster.Builder builder, String hostsString) {
        if (hostsString == null || hostsString.trim().isEmpty()) {
            throw new IllegalArgumentException("Hosts configuration cannot be empty");
        }

        String[] hostEntries = hostsString.split(",");

        for (String hostEntry : hostEntries) {
            String trimmedEntry = hostEntry.trim();
            if (trimmedEntry.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format("Empty host entry found in: %s", hostsString));
            }

            Matcher matcher = HOST_PORT_PATTERN.matcher(trimmedEntry);
            if (!matcher.matches()) {
                throw new IllegalArgumentException(
                        String.format("Invalid host format: %s", trimmedEntry));
            }

            String host = matcher.group(1);
            String portGroup = matcher.group(2);

            if (portGroup != null) {
                // Host with specific port
                int hostPort = Integer.parseInt(portGroup);
                if (hostPort <= 0 || hostPort > 65535) {
                    throw new IllegalArgumentException(
                            String.format("Invalid port number: %s for host: %s", hostPort, host));
                }
                builder.addContactPoint(host).withPort(hostPort);
            } else {
                // Host without port - use configured port
                builder.addContactPoint(host).withPort(this.port);
            }
        }
    }

    private QueryOptions createQueryOptions() {
        return new QueryOptions().setConsistencyLevel(consistencyLevel);
    }

    private SocketOptions createSocketOptions() {
        return new SocketOptions()
                .setConnectTimeoutMillis(validateTimeout(connectTimeoutMs, "connect"))
                .setReadTimeoutMillis(validateTimeout(readTimeoutMs, "read"));
    }

    private int validateTimeout(long timeoutMs, String timeoutType) {
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
        return (int) timeoutMs;
    }

    private ConsistencyLevel parseConsistencyLevel(String level) {
        if (level == null || level.trim().isEmpty()) {
            throw new IllegalArgumentException("Consistency level cannot be empty");
        }

        try {
            return ConsistencyLevel.valueOf(level.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid consistency level: "
                            + level
                            + ". Valid values are: "
                            + String.join(", ", Arrays.toString(ConsistencyLevel.values())),
                    e);
        }
    }
}
