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

package org.apache.flink.connector.cassandra.sink.planner.core.strategy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.planner.core.modifiers.ClauseBindings;
import org.apache.flink.connector.cassandra.sink.planner.core.modifiers.CqlClauseResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ResolvedWrite;
import org.apache.flink.connector.cassandra.sink.planner.resolver.TableRef;

import java.io.Serializable;

/**
 * Strategy interface for generating CQL queries from {@link ResolvedWrite} operations.
 *
 * <p>Each strategy is specialized for one operation type:
 *
 * <ul>
 *   <li>InsertStrategy: generates INSERT statements from resolved writes
 *   <li>UpdateStrategy: generates UPDATE statements from resolved writes
 *   <li>StaticInsertStrategy/StaticUpdateStrategy: returns user-provided queries as-is
 * </ul>
 *
 * @param <INPUT> the input record type
 */
@Internal
public interface PlannerStrategy<INPUT> extends Serializable {

    /**
     * Holds a built CQL query and any clause-originated bind values.
     *
     * <p>This class separates the complete CQL query string from values needed for CQL clauses
     * (TTL, timestamp, IF conditions). Note:
     *
     * <ul>
     *   <li>Column values come from {@link
     *       org.apache.flink.connector.cassandra.sink.planner.resolver.ColumnValueResolver}
     *   <li>Clause values come from {@link CqlClauseResolver} (TTL, timestamp, conditions)
     * </ul>
     *
     * <p><strong>Examples:</strong>
     *
     * <p>Simple INSERT (no clauses):
     *
     * <pre>{@code
     * query: "INSERT INTO users (id, name, age) VALUES (?, ?, ?)"
     * clauseBindings: []  // No TTL, timestamp, or conditions
     * // Final bind order: [id_value, name_value, age_value]
     * }</pre>
     *
     * <p>INSERT with TTL:
     *
     * <pre>{@code
     * query: "INSERT INTO users (id, name, age) VALUES (?, ?, ?) USING TTL ?"
     * clauseBindings: [3600]  // TTL value from CqlClauseResolver
     * // Final bind order: [id_value, name_value, age_value, 3600]
     * }</pre>
     *
     * <p>UPDATE with TTL and Timestamp:
     *
     * <pre>{@code
     * query: "UPDATE users USING TTL ? AND TIMESTAMP ? SET name=?, age=? WHERE id=?"
     * clauseBindings: [7200, 1234567890]  // TTL and timestamp from CqlClauseResolver
     * // Final bind order: [7200, 1234567890, name_value, age_value, id_value]
     * }</pre>
     *
     * <p>Conditional UPDATE with IF:
     *
     * <pre>{@code
     * query: "UPDATE users SET age=? WHERE id=? IF age=?"
     * clauseBindings: [30]  // Expected age for IF condition
     * // Final bind order: [age_value, id_value, 30]
     * }</pre>
     *
     * <p>The {@link PlannerStrategy} method merges column values with clause values in the correct
     * order for the prepared statement.
     */
    final class QueryWithBindings implements Serializable {
        public final String query;
        public final ClauseBindings clauseBindings;

        public QueryWithBindings(String query, ClauseBindings clauseBindings) {
            this.query = query;
            this.clauseBindings = clauseBindings == null ? ClauseBindings.empty() : clauseBindings;
        }
    }

    /**
     * Build query text and collect clause bindings introduced by clauseResolver (if any).
     *
     * @param table the target table information
     * @param config the sink configuration
     * @param clauseResolver resolver for applying CQL modifiers (TTL, timestamp, IF conditions)
     * @param write the resolved write operation containing columns and values
     * @param record the input record for per-record modifier resolution
     * @return the query string and any clause bindings
     */
    QueryWithBindings getQueryWithBindings(
            TableRef table,
            CqlSinkConfig<INPUT> config,
            CqlClauseResolver<INPUT> clauseResolver,
            ResolvedWrite write,
            INPUT record);

    /**
     * Get all bind values in the correct order for the prepared statement.
     *
     * <p>This method combines column values from ResolvedWrite with clause values from
     * getQueryWithBindings() in the exact order required by the prepared statement.
     *
     * <p><strong>Note:</strong> The bind order depends on the query structure:
     *
     * <ul>
     *   <li>INSERT: column values, then USING clause values (TTL, timestamp)
     *   <li>UPDATE with USING: USING values first, then SET, then WHERE, then IF values
     *   <li>UPDATE without USING: SET values, WHERE values, then IF clause values
     * </ul>
     *
     * @param write the resolved write operation
     * @param clauseBindings clause bindings from getQueryWithBindings() method
     * @return array of all values to bind to the prepared statement
     */
    Object[] getOrderedBindValues(ResolvedWrite write, ClauseBindings clauseBindings);
}
