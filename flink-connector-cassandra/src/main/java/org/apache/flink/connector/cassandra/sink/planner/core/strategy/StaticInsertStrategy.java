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

/**
 * Static strategy for INSERT queries that returns the user-provided query string exactly as
 * configured.
 *
 * <p>This strategy is used when users provide their own INSERT query string. The strategy validates
 * that the ResolvedWrite is indeed an INSERT operation and returns the exact query string provided
 * by the user in the configuration.
 *
 * <p>Note: In static mode, any USING/IF clauses must be part of the user-provided query string. The
 * CqlClauseResolver is ignored since we cannot modify the user's query.
 *
 * @param <T> the input record type
 */
@Internal
public final class StaticInsertStrategy<T> implements PlannerStrategy<T> {

    @Override
    public QueryWithBindings getQueryWithBindings(
            TableRef table,
            CqlSinkConfig<T> config,
            CqlClauseResolver<T> clauseResolver,
            ResolvedWrite write,
            T record) {
        Preconditions.checkArgument(!write.isUpdate(), "StaticInsertStrategy got UPDATE: " + write);
        return new QueryWithBindings(config.getQuery(), null);
    }

    @Override
    public Object[] getOrderedBindValues(ResolvedWrite write, ClauseBindings ignored) {
        return write.setValues();
    }
}
