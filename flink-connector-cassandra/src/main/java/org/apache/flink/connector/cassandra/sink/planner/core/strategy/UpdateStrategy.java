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

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

/**
 * Strategy for generating UPDATE statements in dynamic mode.
 *
 * <p>This strategy generates CQL UPDATE statements from resolved write operations. It only handles
 * UPDATE operations and expects the {@code ResolvedWrite} to contain both SET columns/values and
 * WHERE columns/values.
 *
 * <p>Example generated query:
 *
 * <pre>
 * UPDATE keyspace.table SET name = ?, email = ? WHERE id = ?
 * </pre>
 *
 * <p>With modifiers:
 *
 * <pre>
 * UPDATE keyspace.table USING TTL 3600 SET name = ?, email = ? WHERE id = ?
 * </pre>
 *
 * <p><strong>Value Binding Order:</strong> Values are bound in the following order:
 *
 * <ol>
 *   <li>SET values (in the order of SET columns)
 *   <li>WHERE values (in the order of WHERE columns)
 *   <li>Clause values (from CqlClauseResolver, if any)
 * </ol>
 *
 * @param <T> the input record type
 */
@Internal
public final class UpdateStrategy<T> implements PlannerStrategy<T> {

    @Override
    public QueryWithBindings getQueryWithBindings(
            TableRef table,
            CqlSinkConfig<T> config,
            CqlClauseResolver<T> cqlClauseResolver,
            ResolvedWrite write,
            T record) {
        Preconditions.checkArgument(write.isUpdate(), "UpdateStrategy received INSERT: " + write);
        Update update = QueryBuilder.update(table.keyspace(), table.tableName());
        // Add SET clauses
        for (String col : write.setColumns()) {
            update.with(QueryBuilder.set(col, QueryBuilder.bindMarker()));
        }
        // Add WHERE clauses
        for (String wc : write.whereColumns()) {
            update.where(QueryBuilder.eq(wc, QueryBuilder.bindMarker()));
        }
        ClauseBindings clauseBindings = cqlClauseResolver.applyTo(update, record);
        return new QueryWithBindings(update.getQueryString(), clauseBindings);
    }

    @Override
    public Object[] getOrderedBindValues(ResolvedWrite write, ClauseBindings clauseBindings) {
        Object[] set = write.setValues();
        Object[] where = write.whereValues();
        Object[] usingVals = clauseBindings.getUsingValues();
        Object[] ifVals = clauseBindings.getIfValues();

        Object[] out = new Object[set.length + where.length + usingVals.length + ifVals.length];
        int p = 0;

        // USING values come first in UPDATE queries
        if (usingVals.length > 0) {
            System.arraycopy(usingVals, 0, out, p, usingVals.length);
            p += usingVals.length;
        }

        // Then SET values
        System.arraycopy(set, 0, out, p, set.length);
        p += set.length;

        // Then WHERE values
        System.arraycopy(where, 0, out, p, where.length);
        p += where.length;

        // Finally IF values
        if (ifVals.length > 0) {
            System.arraycopy(ifVals, 0, out, p, ifVals.length);
        }

        return out;
    }
}
