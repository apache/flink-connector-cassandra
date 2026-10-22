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

package org.apache.flink.connector.cassandra.sink.planner.core.customization;

import org.apache.flink.annotation.PublicEvolving;

import com.datastax.driver.core.Statement;

import java.io.Serializable;

/**
 * Customizes Cassandra statements after they are prepared and bound but before execution.
 *
 * <p>This interface allows for per-statement customization of runtime properties that cannot be set
 * in the CQL text itself. StatementCustomizer operates on individual bound statements at execution
 * time and has access to the original record for record-based decisions.
 *
 * <p><strong>Common Use Cases:</strong>
 *
 * <ul>
 *   <li><strong>Record-based consistency:</strong> Different consistency levels based on record
 *       content
 *   <li><strong>Dynamic idempotency:</strong> Mark operations idempotent based on record type
 *   <li><strong>Conditional timeouts:</strong> Adjust timeouts based on record size or priority
 *   <li><strong>Record-based routing:</strong> Set routing key based on record fields
 * </ul>
 *
 * <p><strong>Example Usage:</strong>
 *
 * <pre>{@code
 * // Example 1: Simple idempotent flag for all statements
 * StatementCustomizer<MyRecord> simpleCustomizer = (statement, record) -> {
 *     statement.setIdempotent(true);
 * };
 *
 * // Example 2: Record-based consistency and timeouts
 * StatementCustomizer<Order> orderCustomizer = (statement, order) -> {
 *     if (order.getPriority() == Priority.CRITICAL) {
 *         statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
 *         statement.setReadTimeoutMillis(5000);
 *     } else {
 *         statement.setConsistencyLevel(ConsistencyLevel.ONE);
 *         statement.setReadTimeoutMillis(2000);
 *     }
 *     if (order.getItemCount() > 100) {
 *         statement.setReadTimeoutMillis(10000);
 *     }
 *
 *     statement.setIdempotent(true);
 * };
 *
 * }</pre>
 *
 * <p>The customizer is applied after value binding, giving access to both the final statement and
 * the original record that generated it.
 *
 * @param <INPUT> the input record type
 */
@PublicEvolving
@FunctionalInterface
public interface StatementCustomizer<INPUT> extends Serializable {

    /**
     * Applies customizations to the bound statement before execution.
     *
     * @param statement the bound statement ready for execution
     * @param record the original input record that generated this statement
     */
    void apply(Statement statement, INPUT record);
}
