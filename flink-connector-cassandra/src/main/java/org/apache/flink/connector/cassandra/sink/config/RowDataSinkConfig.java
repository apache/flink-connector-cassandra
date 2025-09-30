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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

/**
 * Configuration for writing Flink Table/SQL API records ({@link RowData}) to Cassandra.
 *
 * <p>This configuration is used when records are produced from Flink Table/SQL APIs and represented
 * in the internal {@link RowData} format. It includes schema information via {@link DataType} to
 * allow extraction and conversion of individual fields when preparing Cassandra statements.
 *
 * <p><b>Note:</b> The {@code rowDataType} must be the physical row type, not a logical type.
 */
@PublicEvolving
public class RowDataSinkConfig extends CqlSinkConfig<RowData> {

    private static final long serialVersionUID = 1L;

    private final DataType rowDataType;

    /**
     * Private constructor with default settings - use factory method to create instances.
     *
     * @param rowDataType the Flink table schema (physical row type) for input RowData
     */
    private RowDataSinkConfig(DataType rowDataType) {
        super(RecordFormatType.ROWDATA);
        Preconditions.checkArgument(rowDataType != null, "rowDataType cannot be null");
        this.rowDataType = rowDataType;
    }

    /** Creates a RowData sink configuration. */
    public static RowDataSinkConfig forRowData(DataType rowDataType) {
        return new RowDataSinkConfig(rowDataType);
    }

    @Override
    public RowDataSinkConfig withIgnoreNullFields(boolean ignoreNullFields) {
        super.withIgnoreNullFields(ignoreNullFields);
        return this;
    }

    @Override
    public RowDataSinkConfig withQuery(String query) {
        super.withQuery(query);
        return this;
    }

    @Override
    public RowDataSinkConfig withPluggable(SinkPluggable<RowData> pluggable) {
        super.withPluggable(pluggable);
        return this;
    }

    /**
     * Gets the Flink table schema (physical row type) for input RowData.
     *
     * <p>This schema information is used to create appropriate FieldGetter instances for extracting
     * typed field values from RowData records.
     *
     * @return the RowData schema as DataType
     */
    public DataType getRowDataType() {
        return rowDataType;
    }
}
