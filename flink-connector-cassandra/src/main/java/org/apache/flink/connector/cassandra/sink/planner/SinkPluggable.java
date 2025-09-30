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

package org.apache.flink.connector.cassandra.sink.planner;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.cassandra.sink.planner.core.customization.StatementCustomizer;
import org.apache.flink.connector.cassandra.sink.planner.core.modifiers.CqlClauseResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.ColumnValueResolver;
import org.apache.flink.connector.cassandra.sink.planner.resolver.TableResolver;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Holder for all pluggable components that can be customized in the Cassandra sink.
 *
 * <p>All fields are nullable - when null, defaults will be used.
 *
 * @param <INPUT> the input record type
 */
@PublicEvolving
public final class SinkPluggable<INPUT> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TableResolver<INPUT> tableResolver;
    private final ColumnValueResolver<INPUT> columnValueResolver;
    @Nullable private final CqlClauseResolver<INPUT> cqlClauseResolver;
    @Nullable private final StatementCustomizer<INPUT> statementCustomizer;

    SinkPluggable(
            TableResolver<INPUT> tableResolver,
            ColumnValueResolver<INPUT> columnValueResolver,
            @Nullable CqlClauseResolver<INPUT> cqlClauseResolver,
            @Nullable StatementCustomizer<INPUT> statementCustomizer) {
        this.tableResolver = tableResolver;
        this.columnValueResolver = columnValueResolver;
        this.cqlClauseResolver = cqlClauseResolver;
        this.statementCustomizer = statementCustomizer;
    }

    /**
     * Creates a new builder for SinkPluggable.
     *
     * @param <INPUT> the input record type
     * @return a new builder instance
     */
    public static <INPUT> Builder<INPUT> builder() {
        return new Builder<>();
    }

    /**
     * Gets the table resolver for dynamic table selection.
     *
     * @return the table resolver.
     */
    public TableResolver<INPUT> getTableResolver() {
        return tableResolver;
    }

    /**
     * Gets the column value resolver for dynamic column/value extraction.
     *
     * @return the column value resolver.
     */
    public ColumnValueResolver<INPUT> getColumnValueResolver() {
        return columnValueResolver;
    }

    /**
     * Gets the CQL clause resolver for dynamic CQL clause application.
     *
     * @return the CQL clause resolver, or null if not set
     */
    @Nullable
    public CqlClauseResolver<INPUT> getCqlClauseResolver() {
        return cqlClauseResolver;
    }

    /**
     * Gets the statement customizer for per-statement modifications.
     *
     * @return the statement customizer, or null if not set
     */
    @Nullable
    public StatementCustomizer<INPUT> getStatementCustomizer() {
        return statementCustomizer;
    }

    /**
     * Builder for {@link SinkPluggable}.
     *
     * @param <INPUT> the input record type
     */
    @PublicEvolving
    public static class Builder<INPUT> {

        private TableResolver<INPUT> tableResolver;
        private ColumnValueResolver<INPUT> columnValueResolver;
        @Nullable private CqlClauseResolver<INPUT> cqlClauseResolver;
        @Nullable private StatementCustomizer<INPUT> statementCustomizer;
        /**
         * Sets the table resolver for dynamic table selection.
         *
         * @param tableResolver the table resolver to use
         * @return this builder
         */
        public Builder<INPUT> withTableResolver(TableResolver<INPUT> tableResolver) {
            this.tableResolver = tableResolver;
            return this;
        }

        /**
         * Sets the column value resolver for INSERT operations.
         *
         * @param columnValueResolver the resolver to use for INSERT operations
         * @return this builder
         */
        public Builder<INPUT> withColumnValueResolver(
                ColumnValueResolver<INPUT> columnValueResolver) {
            this.columnValueResolver = columnValueResolver;
            return this;
        }

        /**
         * Sets the CQL clause resolver for dynamic CQL clause application.
         *
         * @param cqlClauseResolver the CQL clause resolver to use
         * @return this builder
         */
        public Builder<INPUT> withCqlClauseResolver(
                @Nullable CqlClauseResolver<INPUT> cqlClauseResolver) {
            this.cqlClauseResolver = cqlClauseResolver;
            return this;
        }

        /**
         * Sets the statement customizer for additional statement configuration.
         *
         * @param statementCustomizer the statement customizer to use
         * @return this builder
         */
        public Builder<INPUT> withStatementCustomizer(
                @Nullable StatementCustomizer<INPUT> statementCustomizer) {
            this.statementCustomizer = statementCustomizer;
            return this;
        }

        /**
         * Builds the SinkPluggable instance.
         *
         * @return a new SinkPluggable with the configured components
         */
        public SinkPluggable<INPUT> build() {
            Preconditions.checkArgument(tableResolver != null, "TableResolver cannot be null.");
            Preconditions.checkArgument(
                    columnValueResolver != null, "ColumnValueResolver cannot be null.");
            return new SinkPluggable<>(
                    tableResolver, columnValueResolver, cqlClauseResolver, statementCustomizer);
        }
    }
}
