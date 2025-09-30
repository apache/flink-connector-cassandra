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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Column value resolver for RowData that extracts fields using precomputed FieldGetters.
 *
 * <p>This resolver uses Flink's {@link RowData.FieldGetter} for type-safe field extraction. The
 * getters are precomputed once during construction for optimal performance.
 *
 * <p>Supports both INSERT and UPDATE operations with correct bind-marker ordering:
 *
 * <ul>
 *   <li>INSERT: SET columns in bind-marker order
 *   <li>UPDATE: SET columns first, then WHERE columns in their respective bind-marker orders
 * </ul>
 */
@Internal
public final class RowDataFieldsResolver implements ColumnValueResolver<RowData> {

    private static final long serialVersionUID = 1L;

    private final Kind kind;
    private final List<String> setColumns;
    private final List<String> whereColumns;
    private final RowData.FieldGetter[] setGetters;
    private final RowData.FieldGetter[] whereGetters;

    /** INSERT: all bind markers are SET columns (in order). */
    public RowDataFieldsResolver(RowType rowType, List<String> setColumns) {
        this(rowType, setColumns, Collections.emptyList());
    }

    /** UPDATE: SET bind markers, then WHERE bind markers (each list in order). */
    public RowDataFieldsResolver(
            RowType rowType, List<String> setColumns, List<String> whereColumns) {
        Preconditions.checkNotNull(rowType, "rowType cannot be null");
        Preconditions.checkNotNull(setColumns, "setColumns cannot be null");
        Preconditions.checkNotNull(whereColumns, "whereColumns cannot be null");

        this.kind = whereColumns.isEmpty() ? Kind.INSERT : Kind.UPDATE;
        this.setColumns = setColumns;
        this.whereColumns = whereColumns;

        final Map<String, Integer> nameToIndex = buildNameToIndex(rowType);
        validateColumns(nameToIndex, setColumns, whereColumns);

        this.setGetters = buildGetters(rowType, setColumns, nameToIndex);
        this.whereGetters = buildGetters(rowType, whereColumns, nameToIndex);
    }

    @Override
    public Kind kind() {
        return kind;
    }

    @Override
    public ResolvedWrite resolve(RowData row) {
        final Object[] setVals = extract(setGetters, row);
        if (kind == Kind.INSERT) {
            return ResolvedWrite.insert(setColumns, setVals);
        }
        final Object[] whereVals = extract(whereGetters, row);
        return ResolvedWrite.update(setColumns, setVals, whereColumns, whereVals);
    }

    /**
     * Builds a mapping from column names to their indices in the RowType. This allows O(1) lookup
     * of field positions by name during validation and getter creation.
     *
     * <p>Example:
     *
     * <pre>{@code
     * RowType with fields: ["user_id", "email", "name", "age"]
     * Returns: {"user_id" -> 0, "email" -> 1, "name" -> 2, "age" -> 3}
     * }</pre>
     *
     * @param rowType the row type containing field names and types
     * @return map from column name to field index
     */
    private static Map<String, Integer> buildNameToIndex(RowType rowType) {
        final Map<String, Integer> map = new HashMap<>();
        final List<String> names = rowType.getFieldNames();
        for (int i = 0; i < names.size(); i++) {
            map.put(names.get(i), i);
        }
        return map;
    }

    /**
     * Validates that all requested columns exist in the schema and enforces CQL constraints.
     *
     * <p>Validation rules:
     *
     * <ul>
     *   <li>All SET columns must exist in the RowType schema
     *   <li>All WHERE columns must exist in the RowType schema
     *   <li>No duplicates within SET columns
     *   <li>No duplicates within WHERE columns
     *   <li>No column can appear in both SET and WHERE (Cassandra constraint)
     *   <li>UPDATE operations must have at least one SET column
     * </ul>
     *
     * <p>Examples of validation failures:
     *
     * <pre>{@code
     * // Invalid: Column not in schema
     * setColumns: ["name", "invalid_col"], whereColumns: ["id"]
     * -> Error: "SET column 'invalid_col' not found in RowType schema"
     *
     * // Invalid: Column in both SET and WHERE
     * setColumns: ["id", "name"], whereColumns: ["id"]
     * -> Error: "Column 'id' cannot appear in both SET and WHERE"
     *
     * // Invalid: Duplicate in SET
     * setColumns: ["name", "age", "name"], whereColumns: ["id"]
     * -> Error: "Duplicate column in SET list: 'name'"
     * }</pre>
     *
     * @param nameToIndex mapping from column names to indices
     * @param setColumns columns for the SET clause (or all columns for INSERT)
     * @param whereColumns columns for the WHERE clause (empty for INSERT)
     * @throws IllegalArgumentException if validation fails
     */
    private void validateColumns(
            Map<String, Integer> nameToIndex, List<String> setColumns, List<String> whereColumns) {

        final Set<String> seen = new HashSet<>();
        for (String c : setColumns) {
            Preconditions.checkArgument(
                    nameToIndex.containsKey(c), "SET column '%s' not found in RowType schema", c);
            Preconditions.checkArgument(seen.add(c), "Duplicate column in SET list: '%s'", c);
        }
        for (String c : whereColumns) {
            Preconditions.checkArgument(
                    nameToIndex.containsKey(c), "WHERE column '%s' not found in RowType schema", c);
            Preconditions.checkArgument(
                    seen.add(c), "Column appears in both SET and WHERE: '%s'", c);
        }
        Preconditions.checkArgument(
                whereColumns.isEmpty() || !setColumns.isEmpty(),
                "UPDATE requires at least one SET column");
    }

    /**
     * Creates an array of FieldGetters for efficient value extraction from RowData.
     *
     * <p>FieldGetters are type-aware extractors that handle nulls and type conversions correctly.
     * They are created once during construction and reused for every record, avoiding the overhead
     * of reflection or repeated type checking.
     *
     * <p>Example:
     *
     * <pre>{@code
     * columns: ["email", "age"]  // Bind-marker order from parsed query
     * nameToIndex: {"user_id" -> 0, "email" -> 1, "name" -> 2, "age" -> 3}
     *
     * Returns: [
     *   FieldGetter for field 1 (email, STRING type),
     *   FieldGetter for field 3 (age, INT type)
     * ]
     *
     * // These getters will extract values in the order needed for:
     * // UPDATE users SET email=?, age=? WHERE ...
     * }</pre>
     *
     * @param rowType the row type for determining field types
     * @param columns the columns to create getters for (in bind-marker order)
     * @param nameToIndex mapping from column names to indices
     * @return array of field getters in the same order as columns
     */
    private static RowData.FieldGetter[] buildGetters(
            RowType rowType, List<String> columns, Map<String, Integer> nameToIndex) {
        final RowData.FieldGetter[] gs = new RowData.FieldGetter[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            final int idx = nameToIndex.get(columns.get(i));
            gs[i] = RowData.createFieldGetter(rowType.getTypeAt(idx), idx);
        }
        return gs;
    }

    /**
     * Extracts values from a RowData record using precomputed field getters.
     *
     * <p>This method is called for every record and uses the precomputed getters for efficient
     * extraction. The values are returned in bind-marker order for direct use in prepared
     * statements.
     *
     * <p>Example:
     *
     * <pre>{@code
     * // RowData with: [123, "john@email.com", "John Doe", 30]
     * // Getters for indices: [1, 3] (email and age)
     *
     * Result: ["john@email.com", 30]
     *
     * // These values can be directly bound to:
     * // UPDATE users SET email=?, age=? WHERE id=?
     * }</pre>
     *
     * @param getters the precomputed field getters
     * @param row the RowData record to extract values from
     * @return array of extracted values in bind-marker order
     */
    private static Object[] extract(RowData.FieldGetter[] getters, RowData row) {
        final Object[] out = new Object[getters.length];
        for (int i = 0; i < getters.length; i++) {
            out[i] = getters[i].getFieldOrNull(row);
        }
        return out;
    }
}
