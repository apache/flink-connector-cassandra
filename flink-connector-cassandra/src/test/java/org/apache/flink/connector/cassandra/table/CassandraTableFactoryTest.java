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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.cassandra.session.DefaultCassandraSessionBuilder;
import org.apache.flink.streaming.connectors.cassandra.CassandraSessionBuilder;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link CassandraTableFactory}. */
class CassandraTableFactoryTest {

    private static final ResolvedSchema BASIC_SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.INT().notNull()),
                    Column.physical("name", DataTypes.STRING()),
                    Column.physical("age", DataTypes.INT()),
                    Column.physical("active", DataTypes.BOOLEAN()));

    @Test
    void testCreateTableSource() {
        CassandraTableFactory factory = new CassandraTableFactory();
        assertThat(factory).isNotNull();
        assertThat(factory.factoryIdentifier()).isEqualTo("cassandra");
    }

    @Test
    void testRequiredOptions() {
        CassandraTableFactory factory = new CassandraTableFactory();
        assertThat(factory.requiredOptions()).isEmpty();
    }

    @Test
    void testOptionalOptions() {
        CassandraTableFactory factory = new CassandraTableFactory();
        assertThat(factory.optionalOptions())
                .containsExactlyInAnyOrder(
                        CassandraConnectorOptions.HOSTS,
                        CassandraConnectorOptions.KEYSPACE,
                        CassandraConnectorOptions.TABLE,
                        CassandraConnectorOptions.PORT,
                        CassandraConnectorOptions.LOCAL_DATACENTER,
                        CassandraConnectorOptions.SECURE_CONNECT_BUNDLE,
                        CassandraConnectorOptions.USERNAME,
                        CassandraConnectorOptions.PASSWORD,
                        CassandraConnectorOptions.CONSISTENCY_LEVEL,
                        CassandraConnectorOptions.CONNECT_TIMEOUT,
                        CassandraConnectorOptions.READ_TIMEOUT,
                        CassandraConnectorOptions.QUERY,
                        CassandraConnectorOptions.MAX_SPLIT_MEMORY_SIZE,
                        CassandraConnectorOptions.SESSION_BUILDER_CLASS);
    }

    @Test
    void testFactoryIdentifier() {
        CassandraTableFactory factory = new CassandraTableFactory();
        assertThat(factory.factoryIdentifier()).isEqualTo("cassandra");
    }

    @Test
    void testValidConfigurationWithConnectionDetails() {
        Map<String, String> options = new HashMap<>();
        options.put("hosts", "localhost:9042");
        options.put("keyspace", "test_keyspace");
        options.put("table", "test_table");
        options.put("username", "testuser");
        options.put("password", "testpass");
        options.put("consistency-level", "LOCAL_QUORUM");
        options.put("connect-timeout", "10000"); // 10 seconds in milliseconds
        options.put("read-timeout", "20000"); // 20 seconds in milliseconds
        options.put("max-split-memory-size", "134217728"); // 128MB in bytes

        DynamicTableSource source = createTableSource(options);
        assertThat(source).isNotNull();
        assertThat(source).isInstanceOf(CassandraDynamicTableSource.class);

        // Verify the source was created with correct configuration
        CassandraDynamicTableSource cassandraSource = (CassandraDynamicTableSource) source;
        assertThat(cassandraSource.asSummaryString()).isEqualTo("Cassandra");
        // Verify ChangelogMode is insertOnly as expected
        assertThat(cassandraSource.getChangelogMode()).isEqualTo(ChangelogMode.insertOnly());

        // Verify the actual option values
        assertThat(cassandraSource.getKeyspace()).isEqualTo("test_keyspace");
        assertThat(cassandraSource.getTable()).isEqualTo("test_table");
        assertThat(cassandraSource.getMaxSplitMemorySize()).isEqualTo(134217728L);
        assertThat(cassandraSource.getSessionBuilder())
                .isInstanceOf(DefaultCassandraSessionBuilder.class);
    }

    @Test
    void testValidConfigurationWithCustomQuery() {
        Map<String, String> options = new HashMap<>();
        options.put("hosts", "localhost:9042");
        options.put("query", "SELECT * FROM test_keyspace.test_table WHERE id > 100");
        options.put("username", "testuser");
        options.put("password", "testpass");
        options.put("max-split-memory-size", "268435456"); // 256MB in bytes

        DynamicTableSource source = createTableSource(options);
        assertThat(source).isNotNull();
        assertThat(source).isInstanceOf(CassandraDynamicTableSource.class);

        // Verify the source was created with correct configuration
        CassandraDynamicTableSource cassandraSource = (CassandraDynamicTableSource) source;
        assertThat(cassandraSource.asSummaryString()).isEqualTo("Cassandra");

        // Verify the actual option values
        assertThat(cassandraSource.getQuery())
                .isEqualTo("SELECT * FROM test_keyspace.test_table WHERE id > 100");
        assertThat(cassandraSource.getMaxSplitMemorySize()).isEqualTo(268435456L);
        assertThat(cassandraSource.getSessionBuilder())
                .isInstanceOf(DefaultCassandraSessionBuilder.class);

        // Verify the source can be copied
        DynamicTableSource copiedSource = cassandraSource.copy();
        assertThat(copiedSource).isNotNull();
        assertThat(copiedSource).isInstanceOf(CassandraDynamicTableSource.class);
        assertThat(copiedSource).isEqualTo(cassandraSource);
    }

    @Test
    void testValidConfigurationWithCustomSessionBuilder() {
        Map<String, String> options = new HashMap<>();
        options.put(
                "session-builder-class",
                "org.apache.flink.connector.cassandra.table.CassandraTableFactoryTest$TestSessionBuilder");
        options.put("keyspace", "test_keyspace");
        options.put("table", "test_table");
        options.put("max-split-memory-size", "33554432"); // 32MB in bytes

        DynamicTableSource source = createTableSource(options);
        assertThat(source).isNotNull();
        assertThat(source).isInstanceOf(CassandraDynamicTableSource.class);

        // Verify the source was created with the custom session builder
        CassandraDynamicTableSource cassandraSource = (CassandraDynamicTableSource) source;
        assertThat(cassandraSource.asSummaryString()).isEqualTo("Cassandra");

        // Verify the actual option values
        assertThat(cassandraSource.getKeyspace()).isEqualTo("test_keyspace");
        assertThat(cassandraSource.getTable()).isEqualTo("test_table");
        assertThat(cassandraSource.getMaxSplitMemorySize()).isEqualTo(33554432L);
        assertThat(cassandraSource.getSessionBuilder()).isInstanceOf(TestSessionBuilder.class);

        // Verify it's scan table source and insert-only
        assertThat(cassandraSource).isInstanceOf(ScanTableSource.class);
        assertThat(cassandraSource.getChangelogMode()).isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void testValidConfigurationWithCustomPort() {
        Map<String, String> options = new HashMap<>();
        options.put("hosts", "localhost:7000");
        options.put("port", "7000");
        options.put("keyspace", "test_keyspace");
        options.put("table", "test_table");
        options.put("username", "testuser");
        options.put("password", "testpass");

        DynamicTableSource source = createTableSource(options);
        assertThat(source).isNotNull();
        assertThat(source).isInstanceOf(CassandraDynamicTableSource.class);

        CassandraDynamicTableSource cassandraSource = (CassandraDynamicTableSource) source;
        assertThat(cassandraSource.asSummaryString()).isEqualTo("Cassandra");

        // Verify the actual option values
        assertThat(cassandraSource.getKeyspace()).isEqualTo("test_keyspace");
        assertThat(cassandraSource.getTable()).isEqualTo("test_table");
        assertThat(cassandraSource.getSessionBuilder())
                .isInstanceOf(DefaultCassandraSessionBuilder.class);
        assertThat(cassandraSource.getChangelogMode()).isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void testInvalidConfigurationMissingConnectionDetails() {
        Map<String, String> options = new HashMap<>();
        assertThatThrownBy(() -> createTableSource(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Must provide either 'query' OR both 'keyspace' and 'table'");
    }

    @Test
    void testInvalidConfigurationMissingHosts() {
        Map<String, String> options = new HashMap<>();
        options.put("keyspace", "test_keyspace");
        options.put("table", "test_table");
        options.put("username", "testuser");
        options.put("password", "testpass");
        assertThatThrownBy(() -> createTableSource(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "'hosts' or 'secure-connect-bundle' is required when using default CassandraSessionBuilder");
    }

    @Test
    void testInvalidConfigurationMissingUsername() {
        Map<String, String> options = new HashMap<>();
        options.put("hosts", "localhost:9042");
        options.put("keyspace", "test_keyspace");
        options.put("table", "test_table");
        options.put("password", "testpass");
        assertThatThrownBy(() -> createTableSource(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Username is required when using default CassandraSessionBuilder");
    }

    @Test
    void testInvalidConfigurationMissingPassword() {
        Map<String, String> options = new HashMap<>();
        options.put("hosts", "localhost:9042");
        options.put("keyspace", "test_keyspace");
        options.put("table", "test_table");
        options.put("username", "testuser");
        assertThatThrownBy(() -> createTableSource(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Password is required when using default CassandraSessionBuilder");
    }

    @Test
    void testInvalidConfigurationMissingKeyspace() {
        Map<String, String> options = new HashMap<>();
        options.put("hosts", "localhost:9042");
        options.put("table", "test_table");
        options.put("username", "testuser");
        options.put("password", "testpass");
        assertThatThrownBy(() -> createTableSource(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Must provide either 'query' OR both 'keyspace' and 'table'");
    }

    @Test
    void testLocalDatacenterAndDriverOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("hosts", "localhost:9042");
        options.put("username", "cassandra");
        options.put("password", "cassandra");
        options.put("keyspace", "test_keyspace");
        options.put("table", "test_table");
        options.put("local-datacenter", "dc1");
        options.put("driver.basic.request.page-size", "1000");
        options.put("driver.advanced.request-id.generator.class", "UuidRequestIdGenerator");

        CassandraDynamicTableSource cassandraSource =
                (CassandraDynamicTableSource) createTableSource(options);

        Map<String, String> driverOptions = new HashMap<>();
        driverOptions.put("basic.request.page-size", "1000");
        driverOptions.put("advanced.request-id.generator.class", "UuidRequestIdGenerator");
        assertThat(cassandraSource.getSessionBuilder())
                .isEqualTo(
                        DefaultCassandraSessionBuilder.builder()
                                .setHosts("localhost:9042")
                                .setPort(CassandraConnectorOptions.PORT.defaultValue())
                                .setLocalDatacenter("dc1")
                                .setCredentials("cassandra", "cassandra")
                                .setConsistencyLevel(
                                        CassandraConnectorOptions.CONSISTENCY_LEVEL.defaultValue())
                                .setConnectTimeoutMs(
                                        CassandraConnectorOptions.CONNECT_TIMEOUT.defaultValue())
                                .setRequestTimeoutMs(
                                        CassandraConnectorOptions.READ_TIMEOUT.defaultValue())
                                .setDriverOptions(driverOptions)
                                .build());
    }

    @Test
    void testSecureConnectBundleWithoutHosts() {
        Map<String, String> options = new HashMap<>();
        options.put("secure-connect-bundle", "/path/to/secure-connect-database.zip");
        options.put("username", "token");
        options.put("password", "secret");
        options.put("keyspace", "test_keyspace");
        options.put("table", "test_table");

        CassandraDynamicTableSource cassandraSource =
                (CassandraDynamicTableSource) createTableSource(options);

        assertThat(cassandraSource.getSessionBuilder())
                .isEqualTo(
                        DefaultCassandraSessionBuilder.builder()
                                .setHosts(null)
                                .setPort(CassandraConnectorOptions.PORT.defaultValue())
                                .setSecureConnectBundle("/path/to/secure-connect-database.zip")
                                .setCredentials("token", "secret")
                                .setConsistencyLevel(
                                        CassandraConnectorOptions.CONSISTENCY_LEVEL.defaultValue())
                                .setConnectTimeoutMs(
                                        CassandraConnectorOptions.CONNECT_TIMEOUT.defaultValue())
                                .setRequestTimeoutMs(
                                        CassandraConnectorOptions.READ_TIMEOUT.defaultValue())
                                .build());
    }

    @Test
    void testUnsupportedOptionIsRejected() {
        Map<String, String> options = new HashMap<>();
        options.put("hosts", "localhost:9042");
        options.put("username", "cassandra");
        options.put("password", "cassandra");
        options.put("keyspace", "test_keyspace");
        options.put("table", "test_table");
        options.put("unknown-option", "value");

        assertThatThrownBy(() -> createTableSource(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("unknown-option");
    }

    private DynamicTableSource createTableSource(Map<String, String> options) {
        CassandraTableFactory factory = new CassandraTableFactory();
        ResolvedCatalogTable catalogTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(BASIC_SCHEMA).build(),
                                "Test table",
                                new ArrayList<>(),
                                options),
                        BASIC_SCHEMA);
        DynamicTableFactory.Context context =
                new DynamicTableFactory.Context() {
                    @Override
                    public ObjectIdentifier getObjectIdentifier() {
                        return ObjectIdentifier.of("catalog", "database", "table");
                    }

                    @Override
                    public ResolvedCatalogTable getCatalogTable() {
                        return catalogTable;
                    }

                    @Override
                    public Configuration getConfiguration() {
                        return new Configuration();
                    }

                    @Override
                    public ClassLoader getClassLoader() {
                        return Thread.currentThread().getContextClassLoader();
                    }

                    @Override
                    public boolean isTemporary() {
                        return false;
                    }
                };

        return factory.createDynamicTableSource(context);
    }

    /**
     * Test implementation of CassandraSessionBuilder for testing custom CassandraSessionBuilder
     * scenarios.
     */
    public static class TestSessionBuilder extends CassandraSessionBuilder {
        public TestSessionBuilder() {}

        @Override
        protected CqlSession buildSession(CqlSessionBuilder builder) {
            return builder.addContactPoint(new InetSocketAddress("localhost", 9042))
                    .withLocalDatacenter("datacenter1")
                    .build();
        }
    }
}
