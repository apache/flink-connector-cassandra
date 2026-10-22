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

package org.apache.flink.connector.cassandra.sink;

import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.PojoSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.write.RequestConfiguration;
import org.apache.flink.connector.cassandra.sink.exception.CassandraFailureHandler;
import org.apache.flink.connector.cassandra.sink.planner.SinkPluggable;
import org.apache.flink.connector.cassandra.sink.planner.core.customization.StatementCustomizer;
import org.apache.flink.connector.cassandra.sink.planner.core.modifiers.ClauseBindings;
import org.apache.flink.connector.cassandra.sink.planner.core.modifiers.CqlClauseResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ColumnValueResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ResolvedWrite;
import org.apache.flink.connector.cassandra.sink.planner.resolver.TableRef;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.SimpleMapperOptions;
import org.apache.flink.types.Row;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CassandraSinkBuilder}. */
public class CassandraSinkBuilderTest {

    /** Test POJO class with Cassandra annotations. */
    @Table(keyspace = "test_keyspace", name = "test_table")
    public static class TestPojo {
        @Column(name = "id")
        private String id;

        @Column(name = "name")
        private String name;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    private static class TestClusterBuilder extends ClusterBuilder {
        @Override
        protected Cluster buildCluster(Cluster.Builder builder) {
            return builder.addContactPoint("localhost").build();
        }
    }

    @Test
    void testConnectionConfiguration() {
        CqlSinkConfig<Row> config =
                CqlSinkConfig.forRow()
                        .withQuery("INSERT INTO keyspace.table (id, name) VALUES (?, ?)");

        // Test 1: Host only with default port - verify ALL defaults are set
        CassandraSink<Row> sink1 =
                CassandraSinkBuilder.newBuilder(config).setHost("localhost").build();
        assertThat(sink1).isNotNull();
        assertThat(sink1.getClusterBuilder()).isNotNull();
        assertThat(sink1.getSinkConfig()).isSameAs(config);
        assertThat(sink1.getFailureHandler()).isNotNull();
        assertThat(sink1.getFailureHandler()).isExactlyInstanceOf(CassandraFailureHandler.class);
        assertThat(sink1.getRequestConfiguration()).isNotNull();
        assertThat(sink1.getRequestConfiguration().getMaxRetries()).isEqualTo(0);
        assertThat(sink1.getRequestConfiguration().getMaxConcurrentRequests())
                .isEqualTo(Integer.MAX_VALUE);
        assertThat(sink1.getRequestConfiguration().getMaxTimeout())
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(sink1.getRequestConfiguration().getFlushTimeout())
                .isEqualTo(Duration.ofSeconds(30));

        // Test 2: Host with custom port - verify all components are set
        CassandraSink<Row> sink2 =
                CassandraSinkBuilder.newBuilder(config).setHost("localhost").setPort(9999).build();
        assertThat(sink2).isNotNull();
        assertThat(sink2.getClusterBuilder()).isNotNull();
        assertThat(sink2.getSinkConfig()).isSameAs(config);
        assertThat(sink2.getFailureHandler()).isNotNull();
        assertThat(sink2.getFailureHandler()).isExactlyInstanceOf(CassandraFailureHandler.class);
        assertThat(sink2.getRequestConfiguration()).isNotNull();
        assertThat(sink2.getRequestConfiguration().getMaxRetries()).isEqualTo(0);
        assertThat(sink2.getRequestConfiguration().getMaxConcurrentRequests())
                .isEqualTo(Integer.MAX_VALUE);
        assertThat(sink2.getRequestConfiguration().getMaxTimeout())
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(sink2.getRequestConfiguration().getFlushTimeout())
                .isEqualTo(Duration.ofSeconds(30));

        // Test 3: Custom ClusterBuilder - verify custom builder is used
        TestClusterBuilder customClusterBuilder = new TestClusterBuilder();
        CassandraSink<Row> sink3 =
                CassandraSinkBuilder.newBuilder(config)
                        .setClusterBuilder(customClusterBuilder)
                        .build();
        assertThat(sink3).isNotNull();
        assertThat(sink3.getClusterBuilder()).isSameAs(customClusterBuilder);
        assertThat(sink3.getSinkConfig()).isSameAs(config);
        assertThat(sink3.getFailureHandler()).isNotNull();
        assertThat(sink3.getFailureHandler()).isExactlyInstanceOf(CassandraFailureHandler.class);
        assertThat(sink3.getRequestConfiguration()).isNotNull();
        assertThat(sink3.getRequestConfiguration().getMaxRetries()).isEqualTo(0);
        assertThat(sink3.getRequestConfiguration().getMaxConcurrentRequests())
                .isEqualTo(Integer.MAX_VALUE);
        assertThat(sink3.getRequestConfiguration().getMaxTimeout())
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(sink3.getRequestConfiguration().getFlushTimeout())
                .isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void testBuilderValidation() {
        CqlSinkConfig<Row> config =
                CqlSinkConfig.forRow()
                        .withQuery("INSERT INTO keyspace.table (id, name) VALUES (?, ?)");

        // Connection validation

        // Test 1: No connection info should fail
        assertThatThrownBy(
                        () ->
                                CassandraSinkBuilder.newBuilder(config)
                                        .build()) // No connection info set
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cassandra connection information must be supplied");

        // Test 2: ClusterBuilder then host should fail
        assertThatThrownBy(
                        () ->
                                CassandraSinkBuilder.newBuilder(config)
                                        .setClusterBuilder(new TestClusterBuilder())
                                        .setHost("localhost") // This should fail
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ClusterBuilder was already set");

        // Test 3: ClusterBuilder then port should fail
        assertThatThrownBy(
                        () ->
                                CassandraSinkBuilder.newBuilder(config)
                                        .setClusterBuilder(new TestClusterBuilder())
                                        .setPort(9999) // This should fail
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ClusterBuilder was already set");

        // Test 4: Host then ClusterBuilder should fail
        assertThatThrownBy(
                        () ->
                                CassandraSinkBuilder.newBuilder(config)
                                        .setHost("localhost")
                                        .setClusterBuilder(
                                                new TestClusterBuilder()) // This should fail
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Connection information was already set");

        // Test 5: Port then ClusterBuilder should fail
        assertThatThrownBy(
                        () ->
                                CassandraSinkBuilder.newBuilder(config)
                                        .setPort(9999)
                                        .setClusterBuilder(
                                                new TestClusterBuilder()) // This should fail
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Connection information was already set");

        // Test 6: Port without host should fail on build
        assertThatThrownBy(
                        () ->
                                CassandraSinkBuilder.newBuilder(config)
                                        .setPort(9999) // Port without host
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cassandra connection information must be supplied");

        // Port validation

        // Test 7: Invalid port - zero
        assertThatThrownBy(
                        () ->
                                CassandraSinkBuilder.newBuilder(config)
                                        .setHost("localhost")
                                        .setPort(0) // Invalid port
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("port must be between 1 and 65535");

        // Test 8: Invalid port - negative
        assertThatThrownBy(
                        () ->
                                CassandraSinkBuilder.newBuilder(config)
                                        .setHost("localhost")
                                        .setPort(-1) // Invalid port
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("port must be between 1 and 65535");

        // Test 9: Invalid port - too high
        assertThatThrownBy(
                        () ->
                                CassandraSinkBuilder.newBuilder(config)
                                        .setHost("localhost")
                                        .setPort(65536) // Invalid port
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("port must be between 1 and 65535");

        // Null/empty validation

        // Test 10: Null host
        assertThatThrownBy(() -> CassandraSinkBuilder.newBuilder(config).setHost(null).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host must not be null or empty");

        // Test 11: Empty host
        assertThatThrownBy(() -> CassandraSinkBuilder.newBuilder(config).setHost("").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host must not be null or empty");

        // Test 12: Null ClusterBuilder
        assertThatThrownBy(
                        () ->
                                CassandraSinkBuilder.newBuilder(config)
                                        .setClusterBuilder(null)
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("clusterBuilder must not be null");

        // Test 13: Null FailureHandler
        assertThatThrownBy(
                        () ->
                                CassandraSinkBuilder.newBuilder(config)
                                        .setHost("localhost")
                                        .setFailureHandler(null)
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("failureHandler must not be null");

        // Test 14: Null RequestConfiguration
        assertThatThrownBy(
                        () ->
                                CassandraSinkBuilder.newBuilder(config)
                                        .setHost("localhost")
                                        .setRequestConfiguration(null)
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("requestConfig must not be null");
    }

    @Test
    void testDefaultsInjection() {
        CqlSinkConfig<Row> config =
                CqlSinkConfig.forRow()
                        .withQuery("INSERT INTO keyspace.table (id, name) VALUES (?, ?)");

        CassandraSink<Row> sink =
                CassandraSinkBuilder.newBuilder(config).setHost("localhost").build();

        // Verify defaults were injected
        assertThat(sink.getFailureHandler()).isNotNull();
        assertThat(sink.getFailureHandler()).isExactlyInstanceOf(CassandraFailureHandler.class);
        assertThat(sink.getRequestConfiguration()).isNotNull();
        // Check all default RequestConfiguration values
        assertThat(sink.getRequestConfiguration().getMaxRetries()).isEqualTo(0);
        assertThat(sink.getRequestConfiguration().getMaxConcurrentRequests())
                .isEqualTo(Integer.MAX_VALUE);
        assertThat(sink.getRequestConfiguration().getMaxTimeout()).isEqualTo(Duration.ofMinutes(1));
        assertThat(sink.getRequestConfiguration().getFlushTimeout())
                .isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void testCustomConfigurationPropagation() {
        CqlSinkConfig<Row> config =
                CqlSinkConfig.forRow()
                        .withQuery("INSERT INTO keyspace.table (id, name) VALUES (?, ?)");
        CassandraFailureHandler customHandler = new CassandraFailureHandler();
        RequestConfiguration customRequestConfig =
                RequestConfiguration.builder()
                        .setMaxRetries(5)
                        .setMaxConcurrentRequests(100)
                        .setMaxTimeout(Duration.ofSeconds(10))
                        .setFlushTimeout(Duration.ofSeconds(5))
                        .build();

        CassandraSink<Row> sink =
                CassandraSinkBuilder.newBuilder(config)
                        .setHost("localhost")
                        .setFailureHandler(customHandler)
                        .setRequestConfiguration(customRequestConfig)
                        .build();

        // Verify custom configurations were propagated
        assertThat(sink.getFailureHandler()).isSameAs(customHandler);
        assertThat(sink.getRequestConfiguration()).isSameAs(customRequestConfig);
        assertThat(sink.getSinkConfig()).isSameAs(config);

        // Verify the actual custom values in RequestConfiguration
        assertThat(sink.getRequestConfiguration().getMaxRetries()).isEqualTo(5);
        assertThat(sink.getRequestConfiguration().getMaxConcurrentRequests()).isEqualTo(100);
        assertThat(sink.getRequestConfiguration().getMaxTimeout())
                .isEqualTo(Duration.ofSeconds(10));
        assertThat(sink.getRequestConfiguration().getFlushTimeout())
                .isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void testCqlBuilderWithUnsetModeFails() {
        // Create config in UNSET mode (no query, no pluggable)
        CqlSinkConfig<Row> config = CqlSinkConfig.forRow();

        assertThatThrownBy(
                        () -> CassandraSinkBuilder.newBuilder(config).setHost("localhost").build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "CQL sink configuration requires exactly one mode: either call withQuery(...) for STATIC mode, or withPluggable(...) for DYNAMIC mode.");
    }

    @Test
    void testCqlBuilderModes() {
        // Static mode with query
        CqlSinkConfig<Row> staticConfig =
                CqlSinkConfig.forRow()
                        .withQuery("INSERT INTO keyspace.table (id, name) VALUES (?, ?)");
        CassandraSink<Row> staticSink =
                CassandraSinkBuilder.newBuilder(staticConfig).setHost("localhost").build();
        assertThat(staticSink).isNotNull();
        assertThat(staticSink.getSinkConfig()).isSameAs(staticConfig);

        // Dynamic mode with pluggable - needs resolvers
        SinkPluggable<Row> pluggable =
                SinkPluggable.<Row>builder()
                        .withTableResolver(record -> new TableRef("keyspace", "table"))
                        .withColumnValueResolver(
                                new ColumnValueResolver<Row>() {
                                    @Override
                                    public Kind kind() {
                                        return Kind.INSERT;
                                    }

                                    @Override
                                    public ResolvedWrite resolve(Row record) {
                                        return ResolvedWrite.insert(
                                                Arrays.asList("id", "name"),
                                                new Object[] {
                                                    record.getField(0), record.getField(1)
                                                });
                                    }
                                })
                        .build();

        CqlSinkConfig<Row> dynamicConfig = CqlSinkConfig.forRow().withPluggable(pluggable);
        CassandraSink<Row> dynamicSink =
                CassandraSinkBuilder.newBuilder(dynamicConfig).setHost("localhost").build();
        assertThat(dynamicSink).isNotNull();
        assertThat(dynamicSink.getSinkConfig()).isSameAs(dynamicConfig);
    }

    @Test
    void testPojoBuilderConfiguration() {
        PojoSinkConfig<TestPojo> pojoConfig =
                new PojoSinkConfig<>(TestPojo.class, "test_keyspace", new SimpleMapperOptions());

        CassandraSink<TestPojo> sink =
                CassandraSinkBuilder.newBuilder(pojoConfig)
                        .setHost("localhost")
                        .setPort(9042)
                        .build();

        assertThat(sink).isNotNull();
        assertThat(sink.getSinkConfig()).isSameAs(pojoConfig);
        assertThat(sink.getClusterBuilder()).isNotNull();
        assertThat(sink.getFailureHandler()).isNotNull();
        assertThat(sink.getFailureHandler()).isExactlyInstanceOf(CassandraFailureHandler.class);
        assertThat(sink.getRequestConfiguration()).isNotNull();
        // Check default RequestConfiguration values
        assertThat(sink.getRequestConfiguration().getMaxRetries()).isEqualTo(0);
        assertThat(sink.getRequestConfiguration().getMaxConcurrentRequests())
                .isEqualTo(Integer.MAX_VALUE);
        assertThat(sink.getRequestConfiguration().getMaxTimeout()).isEqualTo(Duration.ofMinutes(1));
        assertThat(sink.getRequestConfiguration().getFlushTimeout())
                .isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void testSinkPluggableValidation() {
        // Test 1: SinkPluggable without resolvers should fail
        assertThatThrownBy(() -> SinkPluggable.<Row>builder().build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TableResolver cannot be null");

        // Test 2: SinkPluggable with only TableResolver should fail
        assertThatThrownBy(
                        () ->
                                SinkPluggable.<Row>builder()
                                        .withTableResolver(
                                                record -> new TableRef("keyspace", "table"))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ColumnValueResolver cannot be null");

        // Test 3: Valid minimal SinkPluggable (required resolvers only)
        SinkPluggable<Row> minimalPluggable =
                SinkPluggable.<Row>builder()
                        .withTableResolver(record -> new TableRef("keyspace", "table"))
                        .withColumnValueResolver(
                                new ColumnValueResolver<Row>() {
                                    @Override
                                    public Kind kind() {
                                        return Kind.INSERT;
                                    }

                                    @Override
                                    public ResolvedWrite resolve(Row record) {
                                        return ResolvedWrite.insert(
                                                Arrays.asList("id", "name"),
                                                new Object[] {
                                                    record.getField(0), record.getField(1)
                                                });
                                    }
                                })
                        .build();
        assertThat(minimalPluggable).isNotNull();

        // Test 4: SinkPluggable with all optional resolvers
        SinkPluggable<Row> fullPluggable =
                SinkPluggable.<Row>builder()
                        .withTableResolver(record -> new TableRef("keyspace", "table"))
                        .withColumnValueResolver(
                                new ColumnValueResolver<Row>() {
                                    @Override
                                    public Kind kind() {
                                        return Kind.UPDATE;
                                    }

                                    @Override
                                    public ResolvedWrite resolve(Row record) {
                                        return ResolvedWrite.update(
                                                Arrays.asList("name"),
                                                        new Object[] {record.getField(1)},
                                                Arrays.asList("id"),
                                                        new Object[] {record.getField(0)});
                                    }
                                })
                        .withCqlClauseResolver(
                                new CqlClauseResolver<Row>() {
                                    @Override
                                    public ClauseBindings applyTo(Insert insert, Row record) {
                                        insert.using(QueryBuilder.ttl(3600));
                                        return ClauseBindings.empty();
                                    }

                                    @Override
                                    public ClauseBindings applyTo(Update update, Row record) {
                                        update.using(QueryBuilder.ttl(3600));
                                        return ClauseBindings.empty();
                                    }
                                })
                        .withStatementCustomizer(
                                (StatementCustomizer<Row>)
                                        (statement, record) -> {
                                            statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
                                            statement.setIdempotent(true);
                                        })
                        .build();
        assertThat(fullPluggable).isNotNull();

        // Test 5: Verify we can use the pluggable in a sink
        CqlSinkConfig<Row> pluggableConfig = CqlSinkConfig.forRow().withPluggable(fullPluggable);
        CassandraSink<Row> sink =
                CassandraSinkBuilder.newBuilder(pluggableConfig).setHost("localhost").build();
        assertThat(sink).isNotNull();
        assertThat(sink.getSinkConfig()).isSameAs(pluggableConfig);
    }

    @Test
    void testValidPortRangeAccepted() {
        CassandraSinkBuilder.CqlBuilder<Row> builder1 =
                CassandraSinkBuilder.newBuilder(
                        CqlSinkConfig.forRow()
                                .withQuery("INSERT INTO keyspace.table (id, name) VALUES (?, ?)"));
        builder1.setPort(1); // Minimum valid
        assertThat(builder1).isNotNull();

        CassandraSinkBuilder.CqlBuilder<Row> builder2 =
                CassandraSinkBuilder.newBuilder(
                        CqlSinkConfig.forRow()
                                .withQuery("INSERT INTO keyspace.table (id, name) VALUES (?, ?)"));
        builder2.setPort(65535); // Maximum valid
        assertThat(builder2).isNotNull();

        CassandraSinkBuilder.CqlBuilder<Row> builder3 =
                CassandraSinkBuilder.newBuilder(
                        CqlSinkConfig.forRow()
                                .withQuery("INSERT INTO keyspace.table (id, name) VALUES (?, ?)"));
        builder3.setPort(9042); // Default Cassandra port
        assertThat(builder3).isNotNull();
    }
}
