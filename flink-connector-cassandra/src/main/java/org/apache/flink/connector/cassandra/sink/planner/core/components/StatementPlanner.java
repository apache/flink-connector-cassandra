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

package org.apache.flink.connector.cassandra.sink.planner.core.components;

import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.planner.cache.PreparedStatementCache;
import org.apache.flink.connector.cassandra.sink.planner.core.customization.NoOpCustomizer;
import org.apache.flink.connector.cassandra.sink.planner.core.customization.StatementCustomizer;
import org.apache.flink.connector.cassandra.sink.planner.core.modifiers.CqlClauseResolver;
import org.apache.flink.connector.cassandra.sink.planner.core.modifiers.NoOpClauseResolver;
import org.apache.flink.connector.cassandra.sink.planner.core.strategy.PlannerStrategy;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ColumnValueResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ResolvedWrite;
import org.apache.flink.connector.cassandra.sink.planner.resolver.TableRef;
import org.apache.flink.connector.cassandra.sink.planner.resolver.TableResolver;
import org.apache.flink.connector.cassandra.sink.util.CqlStatementHelper;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import javax.annotation.Nullable;

/**
 * Orchestrates the statement planning process by coordinating table resolution, column/value
 * resolution, prepared statement caching, and value binding.
 *
 * <p>This is the core component that implements the planning algorithm:
 *
 * <ol>
 *   <li>Resolve target table for the record
 *   <li>Resolve columns and extract values for the record
 *   <li>Get or create prepared statement for table+columns combination
 *   <li>Bind values to prepared statement
 * </ol>
 *
 * @param <INPUT> the input record type
 */
public class StatementPlanner<INPUT> implements AutoCloseable {

    private final TableResolver<INPUT> tableResolver;
    private final ColumnValueResolver<INPUT> columnValueResolver;
    private final PlannerStrategy<INPUT> strategy;
    private final PreparedStatementCache preparedStatementCache;
    private final CqlClauseResolver<INPUT> cqlClauseResolver;
    private final StatementCustomizer<INPUT> statementCustomizer;

    /**
     * Creates a statement planner with the given components.
     *
     * @param tableResolver resolves target table for records
     * @param columnValueResolver resolves columns and values for records
     * @param strategy handles mode-specific query generation and caching
     * @param cqlClauseResolver resolves CQL modifiers (TTL, timestamp, IF conditions) - can be null
     * @param statementCustomizer optional statement customizer - can be null
     */
    public StatementPlanner(
            TableResolver<INPUT> tableResolver,
            ColumnValueResolver<INPUT> columnValueResolver,
            PlannerStrategy<INPUT> strategy,
            @Nullable CqlClauseResolver<INPUT> cqlClauseResolver,
            @Nullable StatementCustomizer<INPUT> statementCustomizer) {
        Preconditions.checkArgument(tableResolver != null, "tableResolver cannot be null");
        Preconditions.checkArgument(
                columnValueResolver != null, "columnValueResolver cannot be null");
        Preconditions.checkNotNull(strategy != null, "strategy cannot be null");
        this.strategy = strategy;
        this.tableResolver = tableResolver;
        this.columnValueResolver = columnValueResolver;
        this.preparedStatementCache = new PreparedStatementCache();
        this.cqlClauseResolver =
                cqlClauseResolver == null ? new NoOpClauseResolver<>() : cqlClauseResolver;
        this.statementCustomizer =
                statementCustomizer == null ? new NoOpCustomizer<>() : statementCustomizer;
    }

    /**
     * Plans and creates a complete Cassandra Statement for a given input record.
     *
     * <p>This method orchestrates the complete process of converting an input record into a
     * ready-to-execute Cassandra Statement by performing the following steps in order:
     *
     * <ol>
     *   <li><b>Table Resolution:</b> Determines target keyspace.table for the record. Resolve
     *       target table (keyspace.tablename) for this record
     *   <li><b>Column/Value Extraction:</b> Resolves which columns to write and their values
     *   <li><b>Query Generation & Caching:</b> Generates CQL query (INSERT/UPDATE) with modifiers
     *       and caches prepared statement
     *   <li><b>Value Binding:</b> Binds extracted values to prepared statement parameters
     *   <li><b>Statement Customization:</b> Applies statement-level settings like consistency,
     *       timeouts (if customizer set)
     * </ol>
     */
    public Statement plan(INPUT record, Session session, CqlSinkConfig<INPUT> config) {
        TableRef targetTable = tableResolver.resolve(record);
        ResolvedWrite resolvedWrite = columnValueResolver.resolve(record);
        PlannerStrategy.QueryWithBindings qb =
                strategy.getQueryWithBindings(
                        targetTable, config, cqlClauseResolver, resolvedWrite, record);
        PreparedStatement preparedStatement =
                preparedStatementCache.getOrPrepare(session, qb.query);
        Object[] bindingValues = strategy.getOrderedBindValues(resolvedWrite, qb.clauseBindings);
        Statement boundStatement = CqlStatementHelper.bind(preparedStatement, bindingValues);
        statementCustomizer.apply(boundStatement, record);
        return boundStatement;
    }

    /** Clears the prepared statement cache to release resources. */
    public void close() {
        if (preparedStatementCache != null) {
            preparedStatementCache.close();
        }
    }
}
