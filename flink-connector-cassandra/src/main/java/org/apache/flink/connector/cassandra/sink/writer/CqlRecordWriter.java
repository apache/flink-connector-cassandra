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

package org.apache.flink.connector.cassandra.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.planner.core.components.StatementPlanner;
import org.apache.flink.connector.cassandra.sink.planner.core.components.StatementPlannerFactory;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

/**
 * A {@link CassandraRecordWriter} implementation that writes structured records (e.g., Tuple, Row,
 * Scala Product) to Cassandra using explicit CQL insert queries.
 *
 * <p>This writer uses a user-provided or auto-generated {@link PreparedStatement} and binds
 * extracted field values at runtime via {@link
 * org.apache.flink.connector.cassandra.sink.util.CqlStatementHelper}.
 *
 * <p>The writer supports:
 *
 * <ul>
 *   <li>Field extraction based on the configured {@link
 *       org.apache.flink.connector.cassandra.sink.config.RecordFormatType}
 *   <li>Auto-generation of CQL INSERT statements from write options
 *   <li>Optional null field suppression to avoid tombstones
 *   <li>Query modifiers like TTL, timestamp, and consistency level
 * </ul>
 *
 * @param <T> the input record type (e.g., Tuple, Row, Scala Product)
 */
@Internal
public class CqlRecordWriter<T> extends AbstractRecordWriter<T> {

    private final Cluster cluster;
    private final Session session;
    private final StatementPlanner<T> statementPlanner;
    private final CqlSinkConfig<T> config;

    /**
     * Creates a new CQL-based record writer for testing.
     *
     * @param session the Cassandra session to use
     * @param statementPlanner the statement planner for query preparation
     * @param config the sink configuration
     */
    @VisibleForTesting
    CqlRecordWriter(
            Session session, StatementPlanner<T> statementPlanner, CqlSinkConfig<T> config) {
        Preconditions.checkArgument(session != null, "Session cannot be null");
        Preconditions.checkArgument(statementPlanner != null, "StatementPlanner cannot be null");
        Preconditions.checkArgument(config != null, "CqlSinkConfig cannot be null");

        this.session = session;
        this.statementPlanner = statementPlanner;
        this.config = config;
        this.cluster = null;
    }

    /**
     * Creates a new CQL-based record writer.
     *
     * @param builder the {@link ClusterBuilder} used to connect to Cassandra
     * @param config the sink config that provides insert query and record format
     * @throws RuntimeException if initialization fails
     */
    public CqlRecordWriter(ClusterBuilder builder, CqlSinkConfig<T> config) {
        Preconditions.checkArgument(config != null, "CqlSinkConfig cannot be null");
        Preconditions.checkArgument(builder != null, "ClusterBuilder cannot be null");
        this.config = config;

        try {
            this.cluster = builder.getCluster();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create Cassandra cluster from ClusterBuilder. "
                            + "Check your cluster configuration (contact points, credentials, etc.)",
                    e);
        }

        try {
            this.session = cluster.connect();
        } catch (Exception e) {
            // Clean up cluster if session connection fails
            if (cluster != null && !cluster.isClosed()) {
                try {
                    cluster.close();
                } catch (Exception closeException) {
                    e.addSuppressed(closeException);
                }
            }
            throw new RuntimeException(
                    "Failed to connect to Cassandra cluster. "
                            + "Check cluster availability and network connectivity",
                    e);
        }

        this.statementPlanner = StatementPlannerFactory.create(config);
    }

    /**
     * Gets the Cassandra session used by this writer.
     *
     * @return the active session
     */
    @Override
    public Session getSession() {
        return session;
    }

    /**
     * Extracts field values from the input record and creates a bound statement.
     *
     * <p>Delegates to the statement planner which orchestrates table resolution, column/value
     * resolution, prepared statement caching, and value binding.
     *
     * @param input the structured record
     * @return a {@link Statement} ready for async execution
     */
    @Override
    public Statement prepareStatement(T input) {
        return statementPlanner.plan(input, session, config);
    }

    /** Cleanup resources when the writer is closed. */
    @Override
    public void close() {
        try {
            if (statementPlanner != null) {
                statementPlanner.close();
            }
        } finally {
            super.close();
        }
    }

    /**
     * Gets the Cassandra cluster instance.
     *
     * @return the cluster used by this writer
     */
    @Override
    protected Cluster getCluster() {
        return cluster;
    }
}
