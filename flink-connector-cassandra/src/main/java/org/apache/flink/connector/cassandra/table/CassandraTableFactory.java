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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import com.datastax.driver.core.querybuilder.Select;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.flink.connector.cassandra.table.CassandraConnectorOptions.CLUSTER_BUILDER_CLASS;
import static org.apache.flink.connector.cassandra.table.CassandraConnectorOptions.CONNECT_TIMEOUT;
import static org.apache.flink.connector.cassandra.table.CassandraConnectorOptions.CONSISTENCY_LEVEL;
import static org.apache.flink.connector.cassandra.table.CassandraConnectorOptions.HOSTS;
import static org.apache.flink.connector.cassandra.table.CassandraConnectorOptions.KEYSPACE;
import static org.apache.flink.connector.cassandra.table.CassandraConnectorOptions.MAX_SPLIT_MEMORY_SIZE;
import static org.apache.flink.connector.cassandra.table.CassandraConnectorOptions.PASSWORD;
import static org.apache.flink.connector.cassandra.table.CassandraConnectorOptions.PORT;
import static org.apache.flink.connector.cassandra.table.CassandraConnectorOptions.QUERY;
import static org.apache.flink.connector.cassandra.table.CassandraConnectorOptions.READ_TIMEOUT;
import static org.apache.flink.connector.cassandra.table.CassandraConnectorOptions.TABLE;
import static org.apache.flink.connector.cassandra.table.CassandraConnectorOptions.USERNAME;

/** Factory for creating {@link CassandraDynamicTableSource} instances. */
@Internal
public class CassandraTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "cassandra";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        // No options are always required - validation happens in createDynamicTableSource
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTS); // Required in practice but validated in createDynamicTableSource
        options.add(KEYSPACE); // Required for default query generation
        options.add(TABLE); // Required for default query generation
        options.add(PORT); // Default: 9042
        options.add(USERNAME); // Required only for default ClusterBuilder
        options.add(PASSWORD); // Required only for default ClusterBuilder
        options.add(CONSISTENCY_LEVEL); // Default: LOCAL_ONE
        options.add(CONNECT_TIMEOUT); // Default: 5s
        options.add(READ_TIMEOUT); // Default: 10s
        options.add(QUERY); // Default: auto-generated SELECT
        options.add(MAX_SPLIT_MEMORY_SIZE); // Default: 64MB
        options.add(CLUSTER_BUILDER_CLASS); // Default: CassandraClusterBuilder
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        // Validate configuration
        validateConfiguration(helper);

        // Extract required configuration
        final String keyspace = helper.getOptions().get(KEYSPACE);
        final String table = helper.getOptions().get(TABLE);
        final String query = helper.getOptions().get(QUERY);
        final Long maxSplitMemorySize = helper.getOptions().get(MAX_SPLIT_MEMORY_SIZE);

        // Build cluster configuration
        final ClusterBuilder clusterBuilder = createClusterBuilder(helper);

        // Get table schema and data type
        final DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        // Build CQL query if not provided
        final String finalQuery =
                (!isEmpty(query)) ? query : buildDefaultQuery(keyspace, table, context);

        return new CassandraDynamicTableSource(
                clusterBuilder, keyspace, table, producedDataType, finalQuery, maxSplitMemorySize);
    }

    /**
     * Builds the default CQL SELECT query when no custom query is provided.
     *
     * <p>Generates a simple SELECT * FROM keyspace.table query with all physical columns.
     *
     * @param keyspace Cassandra keyspace name
     * @param table Cassandra table name
     * @param context Factory context containing table schema information
     * @return Complete CQL SELECT statement as string
     */
    private String buildDefaultQuery(String keyspace, String table, Context context) {
        List<String> physicalFields =
                context.getCatalogTable().getResolvedSchema().getColumns().stream()
                        .filter(Column::isPhysical)
                        .map(Column::getName)
                        .collect(Collectors.toList());

        // Use QueryBuilder for proper CQL construction
        Select selectStatement =
                select().raw(String.join(", ", physicalFields)).from(keyspace, table);

        return selectStatement.toString();
    }

    /**
     * Creates the ClusterBuilder instance for connecting to Cassandra.
     *
     * <p>The method supports two patterns: 1. <strong>Default behavior:</strong> When no
     * 'cluster-builder-class' is specified, uses CassandraClusterBuilder with username/password
     * authentication 2. <strong>Custom behavior:</strong> When 'cluster-builder-class' is provided,
     * loads and instantiates the specified class via reflection
     *
     * <p>Custom ClusterBuilder classes must: - Extend {@link
     * org.apache.flink.streaming.connectors.cassandra.ClusterBuilder} - Have a public no-argument
     * constructor - Handle their own configuration (typically from environment variables)
     *
     * @param helper {@link FactoryUtil.TableFactoryHelper} containing all parsed configuration
     *     options
     * @return ClusterBuilder instance ready for creating Cassandra connections
     * @throws IllegalArgumentException if custom class cannot be loaded/instantiated or if
     *     username/password are missing for default ClusterBuilder
     */
    protected ClusterBuilder createClusterBuilder(FactoryUtil.TableFactoryHelper helper) {
        final String clusterBuilderClass = helper.getOptions().get(CLUSTER_BUILDER_CLASS);
        final String hosts = helper.getOptions().get(HOSTS);
        final Integer port = helper.getOptions().get(PORT);
        final String username = helper.getOptions().get(USERNAME);
        final String password = helper.getOptions().get(PASSWORD);
        final String consistencyLevel = helper.getOptions().get(CONSISTENCY_LEVEL);
        final Long connectTimeoutMs = helper.getOptions().get(CONNECT_TIMEOUT);
        final Long readTimeoutMs = helper.getOptions().get(READ_TIMEOUT);

        // If no custom ClusterBuilder specified, use default implementation
        if (isEmpty(clusterBuilderClass)) {
            return new CassandraClusterBuilder(
                    hosts,
                    port,
                    username,
                    password,
                    consistencyLevel,
                    connectTimeoutMs,
                    readTimeoutMs);
        }

        // Load and instantiate custom ClusterBuilder
        try {
            Class<?> builderClass = Class.forName(clusterBuilderClass);

            // Verify it extends ClusterBuilder
            if (!ClusterBuilder.class.isAssignableFrom(builderClass)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cluster builder class '%s' must extend '%s'",
                                clusterBuilderClass, ClusterBuilder.class.getName()));
            }

            // Try to instantiate using default constructor
            return (ClusterBuilder) builderClass.getDeclaredConstructor().newInstance();

        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    "Cluster builder class not found: " + clusterBuilderClass, e);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to instantiate cluster builder: "
                            + clusterBuilderClass
                            + ". Make sure the class has a public no-argument constructor.",
                    e);
        }
    }

    /**
     * Validates the entire configuration for consistency and completeness.
     *
     * @param helper Factory helper containing all configuration options
     * @throws IllegalArgumentException if configuration is invalid
     */
    private void validateConfiguration(FactoryUtil.TableFactoryHelper helper) {
        final String hosts = helper.getOptions().get(HOSTS);
        final String keyspace = helper.getOptions().get(KEYSPACE);
        final String table = helper.getOptions().get(TABLE);
        final String query = helper.getOptions().get(QUERY);
        final String clusterBuilderClass = helper.getOptions().get(CLUSTER_BUILDER_CLASS);
        final String username = helper.getOptions().get(USERNAME);
        final String password = helper.getOptions().get(PASSWORD);

        boolean hasCustomQuery = !isEmpty(query);
        boolean hasConnectionDetails = !isEmpty(keyspace) && !isEmpty(table);
        boolean hasCustomClusterBuilder = !isEmpty(clusterBuilderClass);

        // Must provide either custom query OR keyspace+table for default query
        if (!hasCustomQuery && !hasConnectionDetails) {
            throw new IllegalArgumentException(
                    "Must provide either 'query' OR both 'keyspace' and 'table' for default query generation");
        }

        // Validate default ClusterBuilder requirements
        if (!hasCustomClusterBuilder) {
            if (isEmpty(hosts)) {
                throw new IllegalArgumentException(
                        "'hosts' is required when using default ClusterBuilder");
            }
            if (isEmpty(username)) {
                throw new IllegalArgumentException(
                        "Username is required when using default CassandraClusterBuilder. "
                                + "Provide 'username' option or use custom 'cluster-builder-class'.");
            }
            if (isEmpty(password)) {
                throw new IllegalArgumentException(
                        "Password is required when using default CassandraClusterBuilder. "
                                + "Provide 'password' option or use custom 'cluster-builder-class'.");
            }
        }
    }
}
