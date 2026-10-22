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

package org.apache.flink.connector.cassandra.sink.planner.resolver;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.cassandra.sink.config.RecordFormatType;
import org.apache.flink.connector.cassandra.sink.util.CqlStatementHelper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Internal resolver for static queries that extracts fields in parameter marker order and returns
 * ResolvedWrite based on the query type.
 *
 * <p>This resolver is used internally when static queries are parsed to determine the parameter
 * order. It extracts values from records in the order they appear as parameter markers in the
 * static query.
 *
 * @param <INPUT> the input record type
 */
@Internal
public final class FixedColumnValueResolver<INPUT> implements ColumnValueResolver<INPUT> {

    private final RecordFormatType recordFormatType;
    private final List<String> setColumns;
    private final List<String> whereColumns;
    private final Kind kind;

    /**
     * Creates a static resolver for INSERT operations (no WHERE columns).
     *
     * @param recordFormatType the record format for field extraction
     * @param setColumns the columns in parameter marker order
     */
    public FixedColumnValueResolver(RecordFormatType recordFormatType, List<String> setColumns) {
        this(recordFormatType, setColumns, Collections.emptyList());
    }

    /**
     * Creates a static resolver for INSERT or UPDATE operations.
     *
     * @param recordFormatType the record format for field extraction
     * @param setColumns the SET columns in parameter marker order
     * @param whereColumns the WHERE columns in parameter marker order (empty for INSERT)
     */
    public FixedColumnValueResolver(
            RecordFormatType recordFormatType, List<String> setColumns, List<String> whereColumns) {
        this.recordFormatType = recordFormatType;
        this.setColumns = setColumns;
        this.whereColumns = whereColumns;
        this.kind = whereColumns.isEmpty() ? Kind.INSERT : Kind.UPDATE;
    }

    @Override
    public Kind kind() {
        return kind;
    }

    @Override
    public ResolvedWrite resolve(INPUT record) {
        // Extract all fields from record - assumes record fields match parameter marker order
        Object[] allValues = CqlStatementHelper.extractFields(record, recordFormatType);
        ResolvedWrite write = null;
        switch (kind) {
            case INSERT:
                // INSERT operation - all values go to SET clause
                write = ResolvedWrite.insert(setColumns, allValues);
                break;
            case UPDATE:
                // UPDATE operation - split values between SET and WHERE clauses
                int setCount = setColumns.size();
                int expectedTotal = getExpectedTotal(setCount, allValues);

                Object[] setValues = Arrays.copyOfRange(allValues, 0, setCount);
                Object[] whereValues = Arrays.copyOfRange(allValues, setCount, expectedTotal);

                write = ResolvedWrite.update(setColumns, setValues, whereColumns, whereValues);
        }
        return write;
    }

    private int getExpectedTotal(int setCount, Object[] allValues) {
        int whereCount = whereColumns.size();
        int expectedTotal = setCount + whereCount;

        // Validate we have enough fields before splitting
        if (allValues.length < expectedTotal) {
            throw new IllegalStateException(
                    String.format(
                            "UPDATE parameter count mismatch: query expects %d parameters (%d SET + %d WHERE) but record has %d fields",
                            expectedTotal, setCount, whereCount, allValues.length));
        }
        return expectedTotal;
    }
}
