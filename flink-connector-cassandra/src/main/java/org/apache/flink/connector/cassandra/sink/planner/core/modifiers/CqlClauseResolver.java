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

package org.apache.flink.connector.cassandra.sink.planner.core.modifiers;

import org.apache.flink.annotation.PublicEvolving;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Update;

import java.io.Serializable;

/**
 * Resolver for CQL clauses that must be part of the query text.
 *
 * <p>This interface handles CQL clauses that must be included in the query string during
 * preparation time, such as:
 *
 * <ul>
 *   <li>USING TTL &lt;expr&gt; - can use bind markers for dynamic values (INSERT only)
 *   <li>USING TIMESTAMP &lt;expr&gt; - can use bind markers for dynamic values (UPDATE only)
 *   <li>IF NOT EXISTS (INSERT only)
 *   <li>IF EXISTS (UPDATE only)
 *   <li>IF &lt;condition&gt; - can use bind markers for condition values
 *   <li>Other USING clauses that are part of CQL syntax
 * </ul>
 *
 * <p><strong>Bind Marker Support:</strong> When using bind markers (placeholders) in clauses,
 * return the corresponding values to ensure proper binding at execution time. Each bind marker
 * (`?`) added to the query must have a corresponding value in the returned array.
 *
 * <p><strong>Note:</strong> The examples use static imports from {@code
 * com.datastax.driver.core.querybuilder.QueryBuilder} for methods like {@code ttl()}, {@code
 * timestamp()}, {@code bindMarker()}, and {@code eq()}.
 *
 * @param <T> the input record type
 */
@PublicEvolving
public interface CqlClauseResolver<T> extends Serializable {

    /**
     * Apply USING/IF clauses to an INSERT statement and return bind values.
     *
     * <p>INSERT statements only support USING clauses (TTL, TIMESTAMP) and IF NOT EXISTS. The IF
     * NOT EXISTS clause doesn't require bind values.
     *
     * <p><strong>Example: Dynamic TTL</strong>
     *
     * <pre>{@code
     * import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
     *
     * public ClauseBindings applyTo(Insert insert, MyRecord record) {
     *     insert.using(ttl(bindMarker()));
     *     return new ClauseBindings(new Object[]{record.getTtl()}, null);
     * }
     * }</pre>
     *
     * @param insert the INSERT statement to modify
     * @param record the input record that may contain clause values
     * @return ClauseBindings with usingValues populated (ifValues will be empty for INSERT)
     */
    default ClauseBindings applyTo(Insert insert, T record) {
        return ClauseBindings.empty();
    }

    /**
     * Apply USING/IF clauses to an UPDATE statement and return bind values.
     *
     * <p>UPDATE statements support both USING clauses (TTL, TIMESTAMP) and IF clauses (conditions).
     * USING clauses appear before SET in the query, IF clauses appear after WHERE.
     *
     * <p><strong>Example 1: USING clause only</strong>
     *
     * <pre>{@code
     * import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
     *
     * public ClauseBindings applyTo(Update update, MyRecord record) {
     *     update.using(ttl(bindMarker()));
     *     return new ClauseBindings(new Object[]{3600}, null);
     * }
     * }</pre>
     *
     * <p><strong>Example 2: IF clause only</strong>
     *
     * <pre>{@code
     * import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
     *
     * public ClauseBindings applyTo(Update update, MyRecord record) {
     *     update.onlyIf(eq("version", bindMarker()));
     *     return new ClauseBindings(null, new Object[]{record.getExpectedVersion()});
     * }
     * }</pre>
     *
     * <p><strong>Example 3: Both USING and IF clauses</strong>
     *
     * <pre>{@code
     * import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
     *
     * public ClauseBindings applyTo(Update update, MyRecord record) {
     *     update.using(ttl(bindMarker()))
     *           .and(timestamp(bindMarker()))
     *           .onlyIf(eq("version", bindMarker()));
     *     return new ClauseBindings(
     *         new Object[]{7200, 1234567890L},  // USING values
     *         new Object[]{5}                    // IF values
     *     );
     * }
     * }</pre>
     *
     * @param update the UPDATE statement to modify
     * @param record the input record that may contain clause values
     * @return ClauseBindings with separated USING and IF values
     */
    default ClauseBindings applyTo(Update update, T record) {
        return ClauseBindings.empty();
    }
}
