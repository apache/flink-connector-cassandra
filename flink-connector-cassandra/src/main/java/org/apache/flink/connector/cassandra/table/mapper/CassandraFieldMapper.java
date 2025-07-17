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

package org.apache.flink.connector.cassandra.table.mapper;

import org.apache.flink.annotation.Internal;

import com.datastax.driver.core.Row;

import java.io.Serializable;

/**
 * Base interface for all Cassandra field mappers that convert values to Flink's internal {@link
 * org.apache.flink.table.types.logical.LogicalTypeRoot}.
 */
@Internal
public interface CassandraFieldMapper extends Serializable {

    /**
     * Extract a field value from a Cassandra Row and convert it to Flink internal format.
     *
     * <p>This method is used when reading from the Cassandra {@link Row} object returned by a
     * query. It handles null checking and uses Cassandra's type-specific getters (getString,
     * getInt, etc.) to safely extract the field value before converting it to Flink's internal
     * representation.
     *
     * @param row the Cassandra {@link Row} object containing the field
     * @param fieldName the name of the field to extract
     * @return the field value converted to Flink internal format, or null if the field is null
     */
    Object extractFromRow(Row row, String fieldName);

    /**
     * Convert a raw value to Flink internal format.
     *
     * <p>This method is used when we already have an extracted value (not from a Row) that needs to
     * be converted to Flink's internal format. Common scenarios include:
     *
     * <ul>
     *   <li>Array elements: converting individual items from a List to Flink format
     *   <li>Map values: converting keys/values from a Map to Flink format
     *   <li>UDT fields: converting individual fields extracted from a UDTValue
     * </ul>
     *
     * <p>Default implementation returns the value as-is (no conversion needed for primitive types).
     * Override this method when conversion to Flink internal format is required.
     *
     * @param value the raw value to convert (String, Integer, UDTValue, etc.)
     * @return the value converted to Flink internal format, or null if the input value is null
     */
    default Object convertValue(Object value) {
        return value;
    }
}
