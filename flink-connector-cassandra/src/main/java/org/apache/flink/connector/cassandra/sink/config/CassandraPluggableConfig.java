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

package org.apache.flink.connector.cassandra.sink.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.cassandra.sink.planner.SinkPluggable;

import javax.annotation.Nullable;

/**
 * Common interface for Cassandra sink configurations that support pluggable components.
 *
 * <p>This interface extends the base {@link CassandraSinkConfig} to add support for customizable
 * components:
 *
 * <ul>
 *   <li>{@link org.apache.flink.connector.cassandra.sink.planner.resolver.TableResolver}
 *   <li>{@link org.apache.flink.connector.cassandra.sink.planner.resolver.ColumnValueResolver}
 *   <li>{@link
 *       org.apache.flink.connector.cassandra.sink.planner.core.customization.StatementCustomizer}
 *   <li>{@link org.apache.flink.connector.cassandra.sink.planner.core.modifiers.CqlClauseResolver}
 * </ul>
 *
 * <p>All concrete CqlSink implementations would use this interface to provide a uniform way for
 * users to plug in custom behavior.
 *
 * @param <INPUT> the input record type
 */
@PublicEvolving
public interface CassandraPluggableConfig<INPUT> extends CassandraSinkConfig<INPUT> {

    /**
     * Gets the pluggable components configuration.
     *
     * @return the sink pluggable containing optional custom implementations, or null if not set
     */
    @Nullable
    SinkPluggable<INPUT> getPluggable();

    /**
     * Sets the pluggable components configuration.
     *
     * @param pluggable the sink pluggable to use
     * @return this configuration for fluent chaining
     */
    CassandraPluggableConfig<INPUT> withPluggable(SinkPluggable<INPUT> pluggable);
}
