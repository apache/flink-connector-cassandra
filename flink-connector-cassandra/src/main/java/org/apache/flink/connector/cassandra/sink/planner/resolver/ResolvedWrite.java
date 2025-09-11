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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Resolved write operation containing columns and values for INSERT or UPDATE statements.
 *
 * <p>This class represents a resolved write operation that can describe either an INSERT or UPDATE.
 * The resolver determines everything about what to write, and the strategy just builds the
 * appropriate CQL query.
 *
 * <h3>For INSERT operations:</h3>
 *
 * <ul>
 *   <li>Use {@link #insert(List, Object[])} factory method
 *   <li>Provides only column names and values to insert
 *   <li>{@code whereColumns} and {@code whereValues} will be empty
 *   <li>Will generate CQL like: {@code INSERT INTO table (id, name, age) VALUES (?, ?, ?)}
 *   <li>Bind order: values in the same order as columns
 * </ul>
 *
 * <h3>For UPDATE operations:</h3>
 *
 * <ul>
 *   <li>Use {@link #update(List, Object[], List, Object[])} factory method
 *   <li>{@code setColumns}/{@code setValues}: columns to update with new values
 *   <li>{@code whereColumns}/{@code whereValues}: primary key columns for WHERE clause
 *   <li>Will generate CQL like: {@code UPDATE table SET name=?, age=? WHERE id=?}
 *   <li>Bind order: SET values first, then WHERE values
 *   <li><strong>Important:</strong> Columns cannot appear in both SET and WHERE (enforced by
 *       validation)
 * </ul>
 *
 * <h3>Example Usage:</h3>
 *
 * <pre>{@code
 * // INSERT operation - all columns go into the INSERT
 * ResolvedWrite insert = ResolvedWrite.insert(
 *     Arrays.asList("id", "name", "age"),
 *     new Object[]{101, "Alice", 30}
 * );
 * // Generates: INSERT INTO table (id, name, age) VALUES (?, ?, ?)
 * // Binds: [101, "Alice", 30]
 *
 * // UPDATE operation - separate SET and WHERE clauses
 * ResolvedWrite update = ResolvedWrite.update(
 *     Arrays.asList("name", "age"),           // SET columns
 *     new Object[]{"Alice Updated", 31},      // SET values
 *     Arrays.asList("id"),                    // WHERE columns (primary key)
 *     new Object[]{101}                       // WHERE values
 * );
 * // Generates: UPDATE table SET name=?, age=? WHERE id=?
 * // Binds: ["Alice Updated", 31, 101] (SET values first, then WHERE values)
 * }</pre>
 */
@PublicEvolving
public final class ResolvedWrite implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<String> setColumns;
    private final Object[] setValues;
    private final List<String> whereColumns;
    private final Object[] whereValues;

    ResolvedWrite(
            List<String> setColumns,
            Object[] setValues,
            List<String> whereColumns,
            Object[] whereValues) {
        // Validate non-null parameters
        this.setColumns = Preconditions.checkNotNull(setColumns, "setColumns cannot be null");
        this.setValues = Preconditions.checkNotNull(setValues, "setValues cannot be null");
        this.whereColumns = Preconditions.checkNotNull(whereColumns, "whereColumns cannot be null");
        this.whereValues = Preconditions.checkNotNull(whereValues, "whereValues cannot be null");

        // Validate equal lengths
        Preconditions.checkArgument(
                setColumns.size() == setValues.length,
                "setColumns size (%s) must equal setValues length (%s)",
                setColumns.size(),
                setValues.length);
        Preconditions.checkArgument(
                whereColumns.size() == whereValues.length,
                "whereColumns size (%s) must equal whereValues length (%s)",
                whereColumns.size(),
                whereValues.length);

        // Validate at least one SET column for meaningful operations
        Preconditions.checkArgument(
                !setColumns.isEmpty(), "setColumns cannot be empty - need at least one column");

        // Validate disjoint column names (SET and WHERE columns should not overlap)
        if (!whereColumns.isEmpty()) {
            Set<String> setColumnSet = new HashSet<>(setColumns);
            for (String whereColumn : whereColumns) {
                Preconditions.checkArgument(
                        !setColumnSet.contains(whereColumn),
                        "Column '%s' cannot appear in both SET and WHERE clauses",
                        whereColumn);
            }
        }
    }

    /**
     * Factory method for creating an INSERT operation.
     *
     * @param setColumns the columns to insert into
     * @param setValues the values to insert
     * @return a ResolvedWrite representing an INSERT operation
     */
    public static ResolvedWrite insert(List<String> setColumns, Object[] setValues) {
        return new ResolvedWrite(setColumns, setValues, Collections.emptyList(), new Object[0]);
    }

    /**
     * Factory method for creating an UPDATE operation.
     *
     * @param setColumns the columns to update (SET clause)
     * @param setValues the values to set
     * @param whereColumns the columns for the WHERE clause (typically primary key columns)
     * @param whereValues the values for the WHERE clause
     * @return a ResolvedWrite representing an UPDATE operation
     */
    public static ResolvedWrite update(
            List<String> setColumns,
            Object[] setValues,
            List<String> whereColumns,
            Object[] whereValues) {
        return new ResolvedWrite(setColumns, setValues, whereColumns, whereValues);
    }

    /**
     * Returns whether this is an UPDATE operation.
     *
     * @return true if this represents an UPDATE operation (has WHERE columns), false if INSERT
     */
    public boolean isUpdate() {
        return !whereColumns.isEmpty();
    }

    /**
     * Returns the columns for the SET clause (INSERT columns for INSERT operations).
     *
     * @return list of column names for SET clause
     */
    public List<String> setColumns() {
        return setColumns;
    }

    /**
     * Returns the values for the SET clause (INSERT values for INSERT operations).
     *
     * @return array of values for SET clause
     */
    public Object[] setValues() {
        return setValues;
    }

    /**
     * Returns the columns for the WHERE clause (empty for INSERT operations).
     *
     * @return list of column names for WHERE clause
     */
    public List<String> whereColumns() {
        return whereColumns;
    }

    /**
     * Returns the values for the WHERE clause (empty for INSERT operations).
     *
     * @return array of values for WHERE clause
     */
    public Object[] whereValues() {
        return whereValues;
    }
}
