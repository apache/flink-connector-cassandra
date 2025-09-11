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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.connector.cassandra.sink.planner.SinkPluggable;
import org.apache.flink.connector.cassandra.sink.planner.core.resolution.ResolutionMode;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

/**
 * Configuration class for CQL-based Cassandra sink configurations.
 *
 * <p>This holds common configuration shared across different structured data types.
 *
 * <p><b>Modes</b>
 *
 * <ul>
 *   <li><b>STATIC</b>: set via {@link #withQuery(String)}; user supplies exact CQL (INSERT/UPDATE).
 *   <li><b>DYNAMIC</b>: set via {@link #withPluggable(SinkPluggable)}; tables/columns resolved per
 *       record by the pluggable resolvers. {@code query} must be unset.
 * </ul>
 *
 * @param <INPUT> the input record type
 */
@PublicEvolving
public class CqlSinkConfig<INPUT> implements CassandraPluggableConfig<INPUT> {

    private static final long serialVersionUID = 1L;

    private Boolean ignoreNullFields;
    private String query;
    private final RecordFormatType recordFormatType;
    private SinkPluggable<INPUT> pluggable;

    /** Default value for ignoreNullFields. */
    private static final Boolean DEFAULT_IGNORE_NULL_FIELDS = Boolean.FALSE;

    /**
     * Creates a new CQL sink configuration with default settings.
     *
     * @param recordFormatType the format type for field extraction
     */
    protected CqlSinkConfig(RecordFormatType recordFormatType) {
        this.ignoreNullFields = DEFAULT_IGNORE_NULL_FIELDS;
        this.query = null;
        this.recordFormatType = recordFormatType;
    }

    /**
     * Gets the resolution mode based on the current configuration state.
     *
     * @return STATIC if query is set, DYNAMIC if pluggable is set, UNSET otherwise
     */
    public ResolutionMode getResolutionMode() {
        if (!StringUtils.isEmpty(query)) {
            return ResolutionMode.STATIC;
        }
        if (pluggable != null) {
            return ResolutionMode.DYNAMIC;
        }
        return ResolutionMode.UNSET;
    }

    /**
     * Gets whether null values should be unset during writes.
     *
     * <p>When true, bound null values are unset on the statement (INSERT or UPDATE), avoiding the
     * creation of tombstones in Cassandra.
     *
     * @return true if null values should be unset
     */
    public boolean getIgnoreNullFields() {
        return ignoreNullFields;
    }

    /**
     * Gets the CQL query statement to execute.
     *
     * @return the CQL query statement (INSERT or UPDATE), or null when using dynamic/pluggable mode
     */
    @Nullable
    public String getQuery() {
        return query;
    }

    @Override
    @Nullable
    public SinkPluggable<INPUT> getPluggable() {
        return pluggable;
    }

    @Override
    public RecordFormatType getRecordFormatType() {
        return recordFormatType;
    }

    @Override
    public CqlSinkConfig<INPUT> withPluggable(SinkPluggable<INPUT> pluggable) {
        Preconditions.checkState(
                getResolutionMode() != ResolutionMode.STATIC,
                "Cannot set pluggables when a query is already configured (STATIC and DYNAMIC are mutually exclusive).");
        Preconditions.checkArgument(pluggable != null, "pluggable cannot be null");
        this.pluggable = pluggable;
        return this;
    }

    /**
     * Sets whether to unset null values during writes.
     *
     * <p>When true, bound null values are unset on the statement (INSERT or UPDATE), avoiding the
     * creation of tombstones in Cassandra.
     *
     * <p>Note: This setting applies only in STATIC mode. In DYNAMIC mode (when using {@link
     * #withPluggable(SinkPluggable)}), control null/unset behavior via a custom {@link
     * org.apache.flink.connector.cassandra.sink.planner.core.customization.StatementCustomizer} in
     * the pluggable configuration.
     *
     * @param ignoreNullFields true to unset null values
     * @return this configuration for fluent chaining
     * @throws IllegalStateException if called when DYNAMIC mode is already configured
     */
    public CqlSinkConfig<INPUT> withIgnoreNullFields(boolean ignoreNullFields) {
        Preconditions.checkState(
                getResolutionMode() != ResolutionMode.DYNAMIC,
                "ignoreNullFields is not supported in DYNAMIC mode; control null/unset behavior via StatementCustomizer in the SinkPluggable.");
        this.ignoreNullFields = ignoreNullFields;
        return this;
    }

    /**
     * Sets the CQL query for manual mode.
     *
     * @param query the CQL statement (INSERT or UPDATE)
     * @return this configuration for fluent chaining
     */
    public CqlSinkConfig<INPUT> withQuery(String query) {
        Preconditions.checkState(
                getResolutionMode() != ResolutionMode.DYNAMIC,
                "Cannot set query when pluggables are already configured (STATIC and DYNAMIC are mutually exclusive).");
        Preconditions.checkArgument(
                query != null && !StringUtils.isEmpty(query.trim()), "query must not be blank");
        this.query = query;
        return this;
    }

    /**
     * Creates a CQL sink configuration for Row records.
     *
     * <p>Operates in Manual mode if {@code withQuery(...)} is called, or Dynamic mode if {@code
     * withPluggable(...)} is called. Exactly one mode must be configured, otherwise the
     * configuration will fail validation during sink creation.
     *
     * @return a new CQL sink configuration for Row records
     */
    public static CqlSinkConfig<Row> forRow() {
        return new CqlSinkConfig<>(RecordFormatType.ROW);
    }

    /**
     * Creates a CQL sink configuration for Tuple records.
     *
     * <p>Operates in Manual mode if {@code withQuery(...)} is called, or Dynamic mode if {@code
     * withPluggable(...)} is called. Exactly one mode must be configured, otherwise the
     * configuration will fail validation during sink creation.
     *
     * @return a new CQL sink configuration for Tuple records
     */
    public static CqlSinkConfig<Tuple> forTuple() {
        return new CqlSinkConfig<>(RecordFormatType.TUPLE);
    }

    /**
     * Creates a CQL sink configuration for Scala Product records.
     *
     * <p>Operates in Manual mode if {@code withQuery(...)} is called, or Dynamic mode if {@code
     * withPluggable(...)} is called. Exactly one mode must be configured, otherwise the
     * configuration will fail validation during sink creation.
     *
     * @return a new CQL sink configuration for Scala Product records
     */
    public static CqlSinkConfig<scala.Product> forScalaProduct() {
        return new CqlSinkConfig<>(RecordFormatType.SCALA_PRODUCT);
    }
}
