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

package org.apache.flink.connector.cassandra.sink.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.connector.cassandra.sink.config.RecordFormatType;
import org.apache.flink.types.Row;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

import javax.annotation.Nullable;

import scala.Product;

/**
 * Helper utility for working with CQL insert statements.
 *
 * <p>This utility supports:
 *
 * <ul>
 *   <li>Extraction of values from structured record formats (Tuple, Row, Scala Product)
 *   <li>Binding extracted values into prepared Cassandra statements
 *   <li>Handling of null fields with optional suppression
 * </ul>
 */
@Internal
public class CqlStatementHelper {

    /**
     * Extracts bindable field values from a supported input record type.
     *
     * @param record the record to extract values from
     * @param formatType the record format type
     * @return an array of values to be bound to a prepared statement
     * @throws IllegalArgumentException if the format type is unsupported
     */
    public static Object[] extractFields(Object record, RecordFormatType formatType) {
        switch (formatType) {
            case TUPLE:
                Tuple tuple = (Tuple) record;
                return extractValues(tuple.getArity(), tuple::getField);
            case ROW:
                Row row = (Row) record;
                return extractValues(row.getArity(), row::getField);
            case SCALA_PRODUCT:
                Product product = (Product) record;
                return extractValues(product.productArity(), product::productElement);
            default:
                throw new IllegalArgumentException("Unsupported RecordFormatType: " + formatType);
        }
    }

    /**
     * Binds values to a prepared statement.
     *
     * @param preparedStatement the prepared statement
     * @param values the values to bind
     * @return the bound statement ready for execution
     */
    public static BoundStatement bind(PreparedStatement preparedStatement, Object[] values) {
        return preparedStatement.bind(values);
    }

    /** Helper method to extract values using a generic field accessor. */
    private static Object[] extractValues(int arity, FieldAccessor accessor) {
        Object[] values = new Object[arity];
        for (int i = 0; i < arity; i++) {
            values[i] = accessor.getField(i);
        }
        return values;
    }

    /** Functional interface for accessing fields by index. */
    @FunctionalInterface
    private interface FieldAccessor {
        @Nullable
        Object getField(int index);
    }
}
