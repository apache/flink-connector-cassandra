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
import org.apache.flink.util.Preconditions;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Strategy for generating INSERT statements in dynamic mode.
 *
 * <p>This strategy generates CQL INSERT statements from resolved write operations. It only handles
 * INSERT operations and expects the {@code ResolvedWrite} to contain SET columns and values (WHERE
 * columns should be empty).
 *
 * <p>Example generated query:
 *
 * <pre>
 * INSERT INTO keyspace.table (id, name, email) VALUES (?, ?, ?)
 * </pre>
 *
 * <p>With modifiers:
 *
 * <pre>
 * INSERT INTO keyspace.table (id, name, email) VALUES (?, ?, ?) USING TTL ? TIMESTAMP ?
 * </pre>
 *
 * <p><strong>Value Binding Order:</strong> Values are bound in the following order:
 *
 * <ol>
 *   <li>Column values (in the order of columns)
 *   <li>Clause values (from CqlClauseResolver, if any)
 * </ol>
 *
 * @param <T> the input record type
 */
@Internal
public final class InsertStrategy<T> implements PlannerStrategy<T> {

    @Override
    public QueryWithBindings getQueryWithBindings(
            TableRef table,
            CqlSinkConfig<T> config,
            CqlClauseResolver<T> clauseResolver,
            ResolvedWrite write,
            T record) {
        Preconditions.checkArgument(!write.isUpdate(), "InsertStrategy received UPDATE: " + write);
        Insert insert = QueryBuilder.insertInto(table.keyspace(), table.tableName());
        for (String col : write.setColumns()) {
            insert.value(col, QueryBuilder.bindMarker());
        }
        ClauseBindings clauseBindings = clauseResolver.applyTo(insert, record);
        return new QueryWithBindings(insert.getQueryString(), clauseBindings);
    }

    @Override
    public Object[] getOrderedBindValues(ResolvedWrite write, ClauseBindings clauseBindings) {
        Object[] set = write.setValues();
        Object[] usingVals = clauseBindings.getUsingValues();
        if (usingVals.length == 0) {
            return set;
        }
        Object[] out = new Object[set.length + usingVals.length];
        System.arraycopy(set, 0, out, 0, set.length);
        System.arraycopy(usingVals, 0, out, set.length, usingVals.length);
        return out;
    }
}
