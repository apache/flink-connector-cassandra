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

import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.cassandra.sink.config.RecordFormatType;
import org.apache.flink.types.Row;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for {@link CqlStatementHelper}. */
public class CqlStatementHelperTest {

    @Mock private PreparedStatement preparedStatement;
    @Mock private BoundStatement boundStatement;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testExtractFieldsTupleVariousArities() {
        // Tuple3 with values returns array in index order
        Tuple3<Integer, String, Double> tuple3 = new Tuple3<>(1, "a", null);
        Object[] result3 = CqlStatementHelper.extractFields(tuple3, RecordFormatType.TUPLE);
        assertThat(result3).containsExactly(1, "a", null);

        // Tuple0 returns empty array
        Tuple0 tuple0 = new Tuple0();
        Object[] result0 = CqlStatementHelper.extractFields(tuple0, RecordFormatType.TUPLE);
        assertThat(result0).isEmpty();

        // Tuple1 with null preserves null
        Tuple1<String> tuple1 = new Tuple1<>(null);
        Object[] result1 = CqlStatementHelper.extractFields(tuple1, RecordFormatType.TUPLE);
        assertThat(result1).containsExactly((Object) null);
    }

    @Test
    void testExtractFieldsRowVariousCases() {
        // Wrong pairing: Row with TUPLE format throws ClassCastException
        Row wrongRow = Row.of(1, 2, 3);
        assertThatThrownBy(() -> CqlStatementHelper.extractFields(wrongRow, RecordFormatType.TUPLE))
                .isInstanceOf(ClassCastException.class);

        // Row with arity N returns values in order
        Row row = Row.of(1, "test", true, 42L);
        Object[] result = CqlStatementHelper.extractFields(row, RecordFormatType.ROW);
        assertThat(result).containsExactly(1, "test", true, 42L);

        // Row with internal nulls preserves nulls
        Row rowWithNulls = Row.of(1, null, "test", null);
        Object[] resultWithNulls =
                CqlStatementHelper.extractFields(rowWithNulls, RecordFormatType.ROW);
        assertThat(resultWithNulls).containsExactly(1, null, "test", null);

        // Row with mixed types preserves types and order
        Row mixedRow = Row.of(42, "string", true, 3.14, 999L, (byte) 7);
        Object[] mixedResult = CqlStatementHelper.extractFields(mixedRow, RecordFormatType.ROW);
        assertThat(mixedResult).containsExactly(42, "string", true, 3.14, 999L, (byte) 7);
        // Verify exact types preserved
        assertThat(mixedResult[0]).isInstanceOf(Integer.class);
        assertThat(mixedResult[1]).isInstanceOf(String.class);
        assertThat(mixedResult[2]).isInstanceOf(Boolean.class);
        assertThat(mixedResult[3]).isInstanceOf(Double.class);
        assertThat(mixedResult[4]).isInstanceOf(Long.class);
        assertThat(mixedResult[5]).isInstanceOf(Byte.class);

        // Empty Row returns empty array
        Row emptyRow = Row.of();
        Object[] emptyResult = CqlStatementHelper.extractFields(emptyRow, RecordFormatType.ROW);
        assertThat(emptyResult).isEmpty();
    }

    @Test
    void testExtractFieldsScalaProductVariousCases() {
        // ScalaProduct3 returns values in order
        TestScalaProduct product3 = new TestScalaProduct("v0", 42, true);
        Object[] result3 =
                CqlStatementHelper.extractFields(product3, RecordFormatType.SCALA_PRODUCT);
        assertThat(result3).containsExactly("v0", 42, true);

        // ScalaProduct0 returns empty array
        TestScalaProduct0 product0 = new TestScalaProduct0();
        Object[] result0 =
                CqlStatementHelper.extractFields(product0, RecordFormatType.SCALA_PRODUCT);
        assertThat(result0).isEmpty();

        // Wrong pairing: Tuple with SCALA_PRODUCT format throws ClassCastException
        Tuple3<String, Integer, Boolean> tuple = new Tuple3<>("a", 1, true);
        assertThatThrownBy(
                        () ->
                                CqlStatementHelper.extractFields(
                                        tuple, RecordFormatType.SCALA_PRODUCT))
                .isInstanceOf(ClassCastException.class);
    }

    @Test
    void testExtractFieldsPojoFormatThrowsIllegalArgumentException() {
        Object record = new Object();

        assertThatThrownBy(() -> CqlStatementHelper.extractFields(record, RecordFormatType.POJO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported RecordFormatType: POJO");
    }

    @Test
    void testExtractFieldsRowDataFormatThrowsIllegalArgumentException() {
        Object record = new Object();

        assertThatThrownBy(() -> CqlStatementHelper.extractFields(record, RecordFormatType.ROWDATA))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported RecordFormatType: ROWDATA");
    }

    @Test
    void testExtractFieldsLargeArityRowReturnsAllValuesInOrder() {
        // Create a row with 100 fields
        Object[] values = new Object[100];
        for (int i = 0; i < 100; i++) {
            values[i] = "field" + i;
        }
        Row largeRow = Row.of(values);

        Object[] result = CqlStatementHelper.extractFields(largeRow, RecordFormatType.ROW);

        assertThat(result).hasSize(100);
        for (int i = 0; i < 100; i++) {
            assertThat(result[i]).isEqualTo("field" + i);
        }
    }

    @Test
    void testExtractFieldsPreservesSameReferences() {
        String sharedString = "shared";
        Integer sharedInt = 42;
        Row row = Row.of(sharedString, sharedInt);

        Object[] result = CqlStatementHelper.extractFields(row, RecordFormatType.ROW);
        assertThat(result[0]).isSameAs(sharedString);
        assertThat(result[1]).isSameAs(sharedInt);
    }

    @Test
    void testBindVariousArrays() {
        // Binds positional array and returns BoundStatement
        Object[] values = {"a", "b", "c"};
        when(preparedStatement.bind(values)).thenReturn(boundStatement);
        BoundStatement result = CqlStatementHelper.bind(preparedStatement, values);
        assertThat(result).isSameAs(boundStatement);
        verify(preparedStatement).bind(values);

        // Binds empty array and returns BoundStatement
        Object[] emptyValues = new Object[0];
        when(preparedStatement.bind(emptyValues)).thenReturn(boundStatement);
        BoundStatement emptyResult = CqlStatementHelper.bind(preparedStatement, emptyValues);
        assertThat(emptyResult).isNotNull();
        assertThat(emptyResult).isSameAs(boundStatement);
        verify(preparedStatement).bind(emptyValues);

        // Binds array with null elements
        Object[] nullValues = {"a", null, "c", null};
        when(preparedStatement.bind(nullValues)).thenReturn(boundStatement);
        BoundStatement nullResult = CqlStatementHelper.bind(preparedStatement, nullValues);
        assertThat(nullResult).isSameAs(boundStatement);
        verify(preparedStatement).bind(nullValues);

        // Binds large array
        Object[] largeValues = new Object[100];
        for (int i = 0; i < 100; i++) {
            largeValues[i] = i;
        }
        when(preparedStatement.bind(largeValues)).thenReturn(boundStatement);
        BoundStatement largeResult = CqlStatementHelper.bind(preparedStatement, largeValues);
        assertThat(largeResult).isSameAs(boundStatement);
        verify(preparedStatement).bind(largeValues);
    }

    // Test helper classes for Scala Product simulation
    static class TestScalaProduct implements scala.Product {
        private final Object[] values;

        TestScalaProduct(Object v0, Object v1, Object v2) {
            this.values = new Object[] {v0, v1, v2};
        }

        @Override
        public int productArity() {
            return 3;
        }

        @Override
        public Object productElement(int n) {
            return values[n];
        }

        @Override
        public boolean canEqual(Object that) {
            return false;
        }
    }

    static class TestScalaProduct0 implements scala.Product {
        @Override
        public int productArity() {
            return 0;
        }

        @Override
        public Object productElement(int n) {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public boolean canEqual(Object that) {
            return false;
        }
    }
}
