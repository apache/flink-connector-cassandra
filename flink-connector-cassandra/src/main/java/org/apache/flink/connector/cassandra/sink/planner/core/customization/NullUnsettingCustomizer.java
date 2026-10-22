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

import org.apache.flink.annotation.Internal;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Statement;

/**
 * Internal customizer that unsets null values in Cassandra bound statements.
 *
 * <p>This customizer implements the null-ignoring behavior when {@code ignoreNullFields=true} in
 * Static {@link org.apache.flink.connector.cassandra.sink.planner.core.resolution.ResolutionMode}.
 * When a bound statement has null values, they are unset, effectively excluding those columns from
 * the INSERT/UPDATE operation.
 *
 * <p><strong>Example 1: INSERT operation</strong>
 *
 * <pre>{@code
 * // Given a Cassandra table:
 * // CREATE TABLE users (id int PRIMARY KEY, name text, email text, phone text);
 *
 * // Case 1: INSERT with null value (without NullUnsettingCustomizer)
 * // Record: User(id=2, name='Bob', email=null, phone='+1234567890')
 * // Generated statement: INSERT INTO users (id, name, email, phone) VALUES (?, ?, ?, ?)
 * // Bound values: [2, 'Bob', null, '+1234567890']
 * // Result in Cassandra: id=2, name='Bob', email=null, phone='+1234567890'
 *
 * // Case 2: INSERT with unset (with NullUnsettingCustomizer)
 * // Same Record: User(id=2, name='Bob', email=null, phone='+1234567890')
 * // Generated statement: INSERT INTO users (id, name, email, phone) VALUES (?, ?, ?, ?)
 * // After customizer: INSERT INTO users (id, name, phone) VALUES (?, ?, ?)
 * // Bound values: [2, 'Bob', '+1234567890']
 * // Result in Cassandra: id=2, name='Bob', phone='+1234567890'
 * // The email column is simply not set
 * }</pre>
 *
 * <p><strong>Example 2: UPDATE operation</strong>
 *
 * <pre>{@code
 * // Given an existing row in Cassandra:
 * // id=1, name='Alice', email='alice@example.com', phone='+1234567890'
 *
 * // Case 1: UPDATE with null value (without NullUnsettingCustomizer)
 * // Record: User(id=1, name='Alice Updated', email=null, phone='+9876543210')
 * // Generated statement: UPDATE users SET name=?, email=?, phone=? WHERE id=?
 * // Bound values: ['Alice Updated', null, '+9876543210', 1]
 * // Result: id=1, name='Alice Updated', email=null (deleted), phone='+9876543210'
 *
 * // Case 2: UPDATE with unset (with NullUnsettingCustomizer)
 * // Same Record: User(id=1, name='Alice Updated', email=null, phone='+9876543210')
 * // Generated statement: UPDATE users SET name=?, email=?, phone=? WHERE id=?
 * // After customizer: UPDATE users SET name=?, phone=? WHERE id=?
 * // Bound values: ['Alice Updated', '+9876543210', 1]
 * // Result: id=1, name='Alice Updated', email='alice@example.com', phone='+9876543210'
 * // The email column keeps its existing value ('alice@example.com')
 * }</pre>
 *
 * <p><strong>Important:</strong> This customizer only works with {@link BoundStatement}s.
 */
@Internal
public class NullUnsettingCustomizer<INPUT> implements StatementCustomizer<INPUT> {

    @Override
    public void apply(Statement statement, INPUT input) {
        if (!(statement instanceof BoundStatement)) {
            return;
        }

        BoundStatement boundStatement = (BoundStatement) statement;
        int variableCount = boundStatement.preparedStatement().getVariables().size();

        for (int i = 0; i < variableCount; i++) {
            if (boundStatement.isSet(i) && boundStatement.isNull(i)) {
                boundStatement.unset(i);
            }
        }
    }
}
