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

import java.io.Serializable;

/**
 * Resolves write operations (INSERT or UPDATE) for CQL-based records.
 *
 * <p>This abstraction allows for dynamic determination of whether to perform an INSERT or UPDATE
 * operation, along with the appropriate columns and values for each operation type.
 *
 * <p><strong>Example Use Cases:</strong>
 *
 * <pre>{@code
 * // Example 1: INSERT operation with Row
 * ColumnValueResolver<Row> insertResolver = record -> {
 *     List<String> columns = Arrays.asList("id", "name", "email");
 *     Object[] values = new Object[] {
 *         record.getField(0),  // id
 *         record.getField(1),  // name
 *         record.getField(2)   // email
 *     };
 *     return ResolvedWrite.insert(columns, values);
 * };
 *
 * // Example 2: UPDATE operation with Row
 * // Note: UPDATE requires non-empty SET clause and non-empty WHERE clause
 * ColumnValueResolver<Row> updateResolver = record -> {
 *     List<String> setColumns = Arrays.asList("name", "email");
 *     Object[] setValues = new Object[] {
 *         record.getField(1),  // name
 *         record.getField(2)   // email
 *     };
 *     List<String> whereColumns = Arrays.asList("id");
 *     Object[] whereValues = new Object[] {
 *         record.getField(0)   // id
 *     };
 *     return ResolvedWrite.update(setColumns, setValues, whereColumns, whereValues);
 * };
 *
 * // Example 3: INSERT operation with POJO
 * public class User {
 *     private int id;
 *     private String name;
 *     private String email;
 *     // getters and setters...
 * }
 *
 * ColumnValueResolver<User> pojoInsertResolver = user -> {
 *     List<String> columns = Arrays.asList("id", "name", "email");
 *     Object[] values = new Object[] {
 *         user.getId(),
 *         user.getName(),
 *         user.getEmail()
 *     };
 *     return ResolvedWrite.insert(columns, values);
 * };
 *
 * // Example 4: UPDATE operation with POJO
 * ColumnValueResolver<User> pojoUpdateResolver = user -> {
 *     List<String> setColumns = Arrays.asList("name", "email");
 *     Object[] setValues = new Object[] {
 *         user.getName(),
 *         user.getEmail()
 *     };
 *     List<String> whereColumns = Arrays.asList("id");
 *     Object[] whereValues = new Object[] {
 *         user.getId()
 *     };
 *     return ResolvedWrite.update(setColumns, setValues, whereColumns, whereValues);
 * };
 *
 * // Example 5: Conditional INSERT vs UPDATE based on record content
 * ColumnValueResolver<Row> conditionalResolver = record -> {
 *     boolean isUpdate = record.getField(3) != null;  // has an update flag
 *
 *     if (isUpdate) {
 *         return ResolvedWrite.update(
 *             Arrays.asList("name", "email"),
 *             new Object[]{record.getField(1), record.getField(2)},
 *             Arrays.asList("id"),
 *             new Object[]{record.getField(0)}
 *         );
 *     } else {
 *         return ResolvedWrite.insert(
 *             Arrays.asList("id", "name", "email"),
 *             new Object[]{record.getField(0), record.getField(1), record.getField(2)}
 *         );
 *     }
 * };
 * }</pre>
 *
 * <p><strong>When to Use:</strong> Use ColumnValueResolver in DYNAMIC mode when you need
 * record-specific operation determination, conditional INSERT/UPDATE logic, field transformation,
 * or computed columns that cannot be expressed with static configuration.
 *
 * @param <INPUT> the input record type
 */
@PublicEvolving
public interface ColumnValueResolver<INPUT> extends Serializable {

    /** Enum defining the type of CQL operation this resolver handles. */
    @PublicEvolving
    enum Kind {
        INSERT,
        UPDATE
    }

    /**
     * Returns the kind of operation this resolver handles.
     *
     * @return the operation kind (INSERT or UPDATE)
     */
    Kind kind();

    /**
     * Resolves a write operation for the given record.
     *
     * <p>The returned ResolvedWrite should match the kind() returned by this resolver.
     *
     * <p><strong>UPDATE Constraints:</strong> For UPDATE operations, the SET clause must contain at
     * least one column-value pair (non-empty), and the WHERE clause must contain at least one
     * column-value pair (non-empty) to identify the row(s) to update.
     *
     * @param record the input record
     * @return the resolved write operation with columns and values
     */
    ResolvedWrite resolve(INPUT record);
}
