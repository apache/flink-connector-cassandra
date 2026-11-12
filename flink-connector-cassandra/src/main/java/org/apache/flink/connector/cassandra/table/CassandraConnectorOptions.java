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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Options for the Cassandra connector.
 *
 * <h3>Configuration Precedence:</h3>
 *
 * <p>When conflicting options are provided, the connector uses the following precedence rules:
 *
 * <p><strong>Connection Configuration (mutually exclusive groups):</strong>
 *
 * <ul>
 *   <li><strong>Priority 1:</strong> {@link #SESSION_BUILDER_CLASS} - if provided, uses custom
 *       CassandraSessionBuilder and ignores {@link #HOSTS}, {@link #PORT}, {@link #USERNAME},
 *       {@link #PASSWORD}
 *   <li><strong>Priority 2:</strong> {@link #HOSTS}, {@link #PORT}, {@link #USERNAME}, {@link
 *       #PASSWORD} - if no session-builder-class, uses default CassandraSessionBuilder
 * </ul>
 *
 * <p><strong>Query Configuration (mutually exclusive groups):</strong>
 *
 * <ul>
 *   <li><strong>Priority 1:</strong> {@link #QUERY} - if provided, uses the custom CQL query
 *       directly and ignores {@link #KEYSPACE}, {@link #TABLE}
 *   <li><strong>Priority 2:</strong> {@link #KEYSPACE} + {@link #TABLE} - if no query,
 *       auto-generates "SELECT * FROM keyspace.table"
 * </ul>
 *
 * <p><strong>Note:</strong> You can provide options from both groups simultaneously. The connector
 * will use the higher priority option and ignore the lower priority ones without error.
 */
@PublicEvolving
public class CassandraConnectorOptions {

    public static final ConfigOption<String> HOSTS =
            ConfigOptions.key("hosts")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Cassandra hosts to connect to, comma-separated (e.g., 'localhost:9042,host2:9042'). "
                                    + "Ignored if 'session-builder-class' is specified.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(9042)
                    .withDescription(
                            "Cassandra port number (default: 9042). Ignored if 'session-builder-class' is specified.");

    public static final ConfigOption<String> LOCAL_DATACENTER =
            ConfigOptions.key("local-datacenter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the datacenter of the hosts, the driver only sends requests to the nodes of this datacenter. "
                                    + "If not specified, the local datacenter is inferred from the hosts. "
                                    + "Ignored if 'session-builder-class' is specified.");

    public static final ConfigOption<String> SECURE_CONNECT_BUNDLE =
            ConfigOptions.key("secure-connect-bundle")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Secure connect bundle of a cloud database such as DataStax Astra, as a path of the local filesystem or a URL. "
                                    + "The bundle must be accessible from all the nodes of the Flink cluster. "
                                    + "When it is specified, 'hosts', 'port' and 'local-datacenter' are ignored. "
                                    + "Ignored if 'session-builder-class' is specified.");

    public static final ConfigOption<String> KEYSPACE =
            ConfigOptions.key("keyspace")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Cassandra keyspace name. Ignored if 'query' is specified.");

    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Cassandra table name. Ignored if 'query' is specified.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Cassandra username for authentication (required). Ignored if 'session-builder-class' is specified.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Cassandra password for authentication (required). Ignored if 'session-builder-class' is specified.");

    public static final ConfigOption<String> CONSISTENCY_LEVEL =
            ConfigOptions.key("consistency-level")
                    .stringType()
                    .defaultValue("LOCAL_ONE")
                    .withDescription("Cassandra consistency level (e.g., ONE, QUORUM, LOCAL_ONE)");

    public static final ConfigOption<Long> CONNECT_TIMEOUT =
            ConfigOptions.key("connect-timeout")
                    .longType()
                    .defaultValue(5000L)
                    .withDescription(
                            "Time limit for establishing a connection to Cassandra nodes (in milliseconds).");

    public static final ConfigOption<Long> READ_TIMEOUT =
            ConfigOptions.key("read-timeout")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription(
                            "Time limit for a query to complete, including its retries (in milliseconds).");

    public static final ConfigOption<String> QUERY =
            ConfigOptions.key("query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "CQL query for scanning the table (e.g., 'SELECT * FROM keyspace.table'). Takes precedence over 'keyspace' and 'table'.");

    public static final ConfigOption<Long> MAX_SPLIT_MEMORY_SIZE =
            ConfigOptions.key("max-split-memory-size")
                    .longType()
                    .defaultValue(67108864L) // 64MB in bytes
                    .withDescription("Maximum memory size for each split (in bytes).");

    public static final ConfigOption<String> SESSION_BUILDER_CLASS =
            ConfigOptions.key("session-builder-class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Fully qualified class name of custom CassandraSessionBuilder implementation. "
                                    + "Must extend org.apache.flink.streaming.connectors.cassandra.CassandraSessionBuilder "
                                    + "and have a public no-argument constructor. "
                                    + "Takes precedence over 'hosts', 'port', 'username', and 'password'. "
                                    + "If not specified, uses default CassandraSessionBuilder with username/password authentication.");

    /**
     * Prefix of the options passed as they are to the configuration of the Cassandra Java driver,
     * for example {@code 'driver.basic.request.page-size' = '1000'}. They take precedence over the
     * other options and are ignored if 'session-builder-class' is specified.
     */
    public static final String DRIVER_OPTIONS_PREFIX = "driver.";

    private CassandraConnectorOptions() {}
}
