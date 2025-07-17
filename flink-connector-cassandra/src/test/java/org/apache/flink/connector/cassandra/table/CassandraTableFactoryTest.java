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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;

import org.junit.jupiter.api.Test;

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
                        CassandraConnectorOptions.USERNAME,
                        CassandraConnectorOptions.PASSWORD,
                        CassandraConnectorOptions.CONSISTENCY_LEVEL,
                        CassandraConnectorOptions.CONNECT_TIMEOUT,
                        CassandraConnectorOptions.READ_TIMEOUT,
                        CassandraConnectorOptions.QUERY,
                        CassandraConnectorOptions.MAX_SPLIT_MEMORY_SIZE,
                        CassandraConnectorOptions.CLUSTER_BUILDER_CLASS);
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

        DynamicTableSource source = createTableSource(options);
        assertThat(source).isNotNull();
        assertThat(source).isInstanceOf(CassandraDynamicTableSource.class);
    }

    @Test
    void testValidConfigurationWithCustomQuery() {
        Map<String, String> options = new HashMap<>();
        options.put("hosts", "localhost:9042");
        options.put("query", "SELECT * FROM test_keyspace.test_table WHERE id > 100");
        options.put("username", "testuser");
        options.put("password", "testpass");

        DynamicTableSource source = createTableSource(options);
        assertThat(source).isNotNull();
        assertThat(source).isInstanceOf(CassandraDynamicTableSource.class);
    }

    @Test
    void testValidConfigurationWithCustomClusterBuilder() {
        Map<String, String> options = new HashMap<>();
        options.put(
                "cluster-builder-class",
                "org.apache.flink.connector.cassandra.table.CassandraTableFactoryTest$TestClusterBuilder");
        options.put("keyspace", "test_keyspace");
        options.put("table", "test_table");
        DynamicTableSource source = createTableSource(options);
        assertThat(source).isNotNull();
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
                .hasMessageContaining("'hosts' is required when using default ClusterBuilder");
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
                        "Username is required when using default CassandraClusterBuilder");
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
                        "Password is required when using default CassandraClusterBuilder");
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

    private DynamicTableSource createTableSource(Map<String, String> options) {
        CassandraTableFactory factory = new CassandraTableFactory();
        ResolvedCatalogTable catalogTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                org.apache.flink.table.api.Schema.newBuilder()
                                        .fromResolvedSchema(BASIC_SCHEMA)
                                        .build(),
                                "Test table",
                                new java.util.ArrayList<>(),
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

    /** Test implementation of ClusterBuilder for testing custom ClusterBuilder scenarios. */
    public static class TestClusterBuilder
            extends org.apache.flink.streaming.connectors.cassandra.ClusterBuilder {
        public TestClusterBuilder() {}

        @Override
        protected com.datastax.driver.core.Cluster buildCluster(
                com.datastax.driver.core.Cluster.Builder builder) {
            return builder.addContactPoint("localhost").withPort(9042).build();
        }
    }
}
