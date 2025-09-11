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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.cassandra.sink.config.CassandraSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.PojoSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.write.RequestConfiguration;
import org.apache.flink.connector.cassandra.sink.exception.CassandraFailureHandler;
import org.apache.flink.connector.cassandra.sink.planner.core.resolution.ResolutionMode;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.core.Cluster;
import org.apache.commons.lang3.StringUtils;

/**
 * Builder for creating {@link CassandraSink} instances.
 *
 * <p>This builder provides two factory methods for creating type-specific builders:
 *
 * <ul>
 *   <li>{@link #newBuilder(PojoSinkConfig)} - Creates a POJO builder.
 *   <li>{@link #newBuilder(CqlSinkConfig)} - Creates a CQL builder with {@link
 *       org.apache.flink.connector.cassandra.sink.planner.SinkPluggable} support
 * </ul>
 *
 * <p>Example usage with POJO:
 *
 * <pre>{@code
 * CassandraSink<MyPojo> sink = CassandraSinkBuilder
 *     .newBuilder(pojoSinkConfig)
 *     .setClusterBuilder(clusterBuilder)
 *     .build();
 * }</pre>
 *
 * <p>Example usage with CQL (STATIC mode):
 *
 * <pre>{@code
 * CqlSinkConfig<Row> cfg = CqlSinkConfig.forRow()
 *     .withQuery("INSERT INTO ks.tbl(id,name,age) VALUES (?,?,?)");
 *
 * CassandraSink<Row> sink = CassandraSinkBuilder
 *     .newBuilder(cfg)
 *     .setClusterBuilder(clusterBuilder)
 *     .build();
 * }</pre>
 *
 * <p>Example usage with CQL (DYNAMIC mode):
 *
 * <pre>{@code
 * CqlSinkConfig<Row> cfg = CqlSinkConfig.forRow()
 *     .withPluggable(
 *         SinkPluggable.<Row>builder()
 *             .withTableResolver(myTableResolver)
 *             .withInsertResolver(myInsertResolver)
 *             .build());
 *
 * CassandraSink<Row> sink = CassandraSinkBuilder
 *     .newBuilder(cfg)
 *     .setClusterBuilder(clusterBuilder)
 *     .build();
 * }</pre>
 */
@PublicEvolving
public final class CassandraSinkBuilder {

    /**
     * Creates a new builder for POJO sink configuration.
     *
     * @param config The POJO sink configuration
     * @param <INPUT> The input record type
     * @return a new POJO builder instance
     */
    public static <INPUT> PojoBuilder<INPUT> newBuilder(PojoSinkConfig<INPUT> config) {
        return new PojoBuilder<>(config);
    }

    /**
     * Creates a new builder for CQL sink configuration.
     *
     * @param config The CQL sink configuration
     * @param <INPUT> The input record type
     * @return a new CQL builder instance
     */
    public static <INPUT> CqlBuilder<INPUT> newBuilder(CqlSinkConfig<INPUT> config) {
        return new CqlBuilder<>(config);
    }

    /**
     * Base builder with shared configuration options for all sink types.
     *
     * @param <INPUT> The input record type
     * @param <SELF> The concrete builder type for fluent API
     */
    public abstract static class BaseBuilder<INPUT, SELF extends BaseBuilder<INPUT, SELF>> {
        protected final CassandraSinkConfig<INPUT> sinkConfig;
        protected ClusterBuilder clusterBuilder;
        protected String host;
        protected Integer port;
        protected CassandraFailureHandler failureHandler;
        protected RequestConfiguration requestConfig;

        protected BaseBuilder(CassandraSinkConfig<INPUT> sinkConfig) {
            this.sinkConfig = Preconditions.checkNotNull(sinkConfig, "sinkConfig must not be null");
        }

        /**
         * Sets the Cassandra host to connect to.
         *
         * @param host the Cassandra host
         * @return this builder
         */
        public SELF setHost(String host) {
            Preconditions.checkArgument(
                    !StringUtils.isEmpty(host), "host must not be null or empty");
            Preconditions.checkArgument(
                    this.clusterBuilder == null,
                    "ClusterBuilder was already set. Either setHost()/setPort() or setClusterBuilder().");
            this.host = host;
            return self();
        }

        /**
         * Sets the Cassandra port to connect to.
         *
         * @param port the Cassandra port
         * @return this builder
         */
        public SELF setPort(int port) {
            Preconditions.checkArgument(
                    port > 0 && port <= 65535, "port must be between 1 and 65535");
            Preconditions.checkArgument(
                    this.clusterBuilder == null,
                    "ClusterBuilder was already set. Either setHost()/setPort() or setClusterBuilder().");
            this.port = port;
            return self();
        }

        /**
         * Sets the ClusterBuilder for this sink.
         *
         * @param clusterBuilder ClusterBuilder to configure the connection to Cassandra
         * @return this builder
         */
        public SELF setClusterBuilder(ClusterBuilder clusterBuilder) {
            Preconditions.checkNotNull(clusterBuilder, "clusterBuilder must not be null");
            Preconditions.checkArgument(
                    this.host == null && this.port == null,
                    "Connection information was already set. Either setHost()/setPort() or setClusterBuilder().");
            this.clusterBuilder = clusterBuilder;
            return self();
        }

        /**
         * Sets the failure handler for this sink.
         *
         * @param failureHandler CassandraFailureHandler that handles failures
         * @return this builder
         */
        public SELF setFailureHandler(CassandraFailureHandler failureHandler) {
            this.failureHandler =
                    Preconditions.checkNotNull(failureHandler, "failureHandler must not be null");
            return self();
        }

        /**
         * Sets the request configuration using a pre-built RequestConfiguration.
         *
         * @param requestConfig a configured RequestConfiguration.
         * @return this builder
         */
        public SELF setRequestConfiguration(RequestConfiguration requestConfig) {
            this.requestConfig =
                    Preconditions.checkNotNull(requestConfig, "requestConfig must not be null");
            return self();
        }

        /**
         * Builds the configured CassandraSink. This builder can only be used once.
         *
         * @return a new CassandraSink instance
         * @throws IllegalStateException if required configuration is missing or builder already
         *     used
         */
        public final CassandraSink<INPUT> build() {
            // Create ClusterBuilder from host/port if not already set
            if (clusterBuilder == null) {
                if (host != null) {
                    final String finalHost = host;
                    final int finalPort = (port != null) ? port : 9042; // Default Cassandra port
                    this.clusterBuilder =
                            new ClusterBuilder() {
                                @Override
                                protected Cluster buildCluster(Cluster.Builder builder) {
                                    return builder.addContactPoint(finalHost)
                                            .withPort(finalPort)
                                            .build();
                                }
                            };
                }
            }

            Preconditions.checkArgument(
                    this.clusterBuilder != null,
                    "Cassandra connection information must be supplied using either setHost()/setPort() or setClusterBuilder().");
            // Default Failure Handler
            if (this.failureHandler == null) {
                this.failureHandler = new CassandraFailureHandler();
            }
            // Default Request Configuration.
            if (this.requestConfig == null) {
                this.requestConfig = RequestConfiguration.builder().build();
            }

            validateSpecificConfiguration();
            return new CassandraSink<>(sinkConfig, clusterBuilder, failureHandler, requestConfig);
        }

        protected abstract SELF self();

        /** Validates builder-specific configuration before building the sink. */
        protected void validateSpecificConfiguration() {}
    }

    /**
     * Builder for POJO sink configurations. This builder uses DataStax Mapper annotations for all
     * metadata.
     *
     * @param <INPUT> The POJO input type
     */
    @PublicEvolving
    public static final class PojoBuilder<INPUT> extends BaseBuilder<INPUT, PojoBuilder<INPUT>> {

        PojoBuilder(PojoSinkConfig<INPUT> config) {
            super(config);
        }

        @Override
        protected PojoBuilder<INPUT> self() {
            return this;
        }
    }

    /**
     * Builder for CQL sink configurations. This builder exposes CQL-specific pluggable options for
     * dynamic behavior.
     *
     * <p>Supports two sink modes:
     *
     * <ul>
     *   <li><b>STATIC</b>: User provides explicit INSERT/UPDATE query.
     *   <li><b>DYNAMIC</b>: Uses pluggable resolvers for runtime table/column resolution.
     * </ul>
     *
     * @param <INPUT> The input record type (Row, Tuple, RowData, etc.)
     */
    @PublicEvolving
    public static final class CqlBuilder<INPUT> extends BaseBuilder<INPUT, CqlBuilder<INPUT>> {
        private final CqlSinkConfig<INPUT> cqlConfig;

        CqlBuilder(CqlSinkConfig<INPUT> config) {
            super(config);
            this.cqlConfig = config;
        }

        @Override
        protected CqlBuilder<INPUT> self() {
            return this;
        }

        @Override
        protected void validateSpecificConfiguration() {
            ResolutionMode resolutionMode = cqlConfig.getResolutionMode();
            if (resolutionMode == ResolutionMode.UNSET) {
                throw new IllegalStateException(
                        "CQL sink configuration requires exactly one mode: either call withQuery(...) for STATIC mode, or withPluggable(...) for DYNAMIC mode.");
            }
        }
    }
}
