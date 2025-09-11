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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.cassandra.sink.config.RecordFormatType;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link FixedColumnValueResolver}. */
class FixedColumnValueResolverTest {

    @Test
    void testInsertWithRow() {
        FixedColumnValueResolver<Row> resolver =
                new FixedColumnValueResolver<>(
                        RecordFormatType.ROW, Arrays.asList("id", "name", "email"));

        assertThat(resolver.kind()).isEqualTo(ColumnValueResolver.Kind.INSERT);

        Row record = Row.of(123, "Alice", "alice@example.com");
        ResolvedWrite result = resolver.resolve(record);

        assertThat(result.isUpdate()).isFalse();
        assertThat(result.setColumns()).containsExactly("id", "name", "email");
        assertThat(result.setValues()).containsExactly(123, "Alice", "alice@example.com");
        assertThat(result.whereColumns()).isEmpty();
        assertThat(result.whereValues()).isEmpty();
    }

    @Test
    void testInsertWithTuple() {
        FixedColumnValueResolver<Tuple3<Integer, String, String>> resolver =
                new FixedColumnValueResolver<>(
                        RecordFormatType.TUPLE, Arrays.asList("id", "name", "email"));

        assertThat(resolver.kind()).isEqualTo(ColumnValueResolver.Kind.INSERT);

        Tuple3<Integer, String, String> record = new Tuple3<>(123, "Bob", "bob@example.com");
        ResolvedWrite result = resolver.resolve(record);

        assertThat(result.isUpdate()).isFalse();
        assertThat(result.setColumns()).containsExactly("id", "name", "email");
        assertThat(result.setValues()).containsExactly(123, "Bob", "bob@example.com");
        assertThat(result.whereColumns()).isEmpty();
        assertThat(result.whereValues()).isEmpty();
    }

    @Test
    void testInsertSingleColumn() {
        FixedColumnValueResolver<Row> resolver =
                new FixedColumnValueResolver<>(
                        RecordFormatType.ROW, Collections.singletonList("id"));

        Row record = Row.of(456);
        ResolvedWrite result = resolver.resolve(record);

        assertThat(result.setColumns()).containsExactly("id");
        assertThat(result.setValues()).containsExactly(456);
    }

    @Test
    void testInsertFieldCountMismatch() {
        FixedColumnValueResolver<Row> resolver =
                new FixedColumnValueResolver<>(RecordFormatType.ROW, Arrays.asList("id", "name"));

        Row record = Row.of(123, "Alice", "extra_field");

        assertThatThrownBy(() -> resolver.resolve(record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("setColumns size (2) must equal setValues length (3)");
    }

    @Test
    void testUpdateWithRow() {
        FixedColumnValueResolver<Row> resolver =
                new FixedColumnValueResolver<>(
                        RecordFormatType.ROW,
                        Arrays.asList("name", "email"), // SET columns
                        Arrays.asList("id", "version")); // WHERE columns

        assertThat(resolver.kind()).isEqualTo(ColumnValueResolver.Kind.UPDATE);

        Row record = Row.of("Alice Updated", "alice.updated@example.com", 123, 1);
        ResolvedWrite result = resolver.resolve(record);

        assertThat(result.isUpdate()).isTrue();
        assertThat(result.setColumns()).containsExactly("name", "email");
        assertThat(result.setValues())
                .containsExactly("Alice Updated", "alice.updated@example.com");
        assertThat(result.whereColumns()).containsExactly("id", "version");
        assertThat(result.whereValues()).containsExactly(123, 1);
    }

    @Test
    void testUpdateWithTuple() {
        // Tuple4 for: name, email (SET), id, version (WHERE)
        FixedColumnValueResolver<Tuple3<String, Integer, Integer>> resolver =
                new FixedColumnValueResolver<>(
                        RecordFormatType.TUPLE,
                        Collections.singletonList("name"), // SET columns
                        Arrays.asList("id", "version")); // WHERE columns

        Tuple3<String, Integer, Integer> record = new Tuple3<>("Bob Updated", 123, 2);
        ResolvedWrite result = resolver.resolve(record);

        assertThat(result.isUpdate()).isTrue();
        assertThat(result.setColumns()).containsExactly("name");
        assertThat(result.setValues()).containsExactly("Bob Updated");
        assertThat(result.whereColumns()).containsExactly("id", "version");
        assertThat(result.whereValues()).containsExactly(123, 2);
    }

    @Test
    void testUpdateSingleColumns() {
        FixedColumnValueResolver<Row> resolver =
                new FixedColumnValueResolver<>(
                        RecordFormatType.ROW,
                        Collections.singletonList("status"), // SET
                        Collections.singletonList("id")); // WHERE

        Row record = Row.of("active", 789);
        ResolvedWrite result = resolver.resolve(record);

        assertThat(result.setColumns()).containsExactly("status");
        assertThat(result.setValues()).containsExactly("active");
        assertThat(result.whereColumns()).containsExactly("id");
        assertThat(result.whereValues()).containsExactly(789);
    }

    @Test
    void testUpdateFieldCountMismatch() {
        FixedColumnValueResolver<Row> resolver =
                new FixedColumnValueResolver<>(
                        RecordFormatType.ROW,
                        Arrays.asList("name", "email"), // 2 SET columns
                        Collections.singletonList("id")); // 1 WHERE column (total: 3)

        Row record = Row.of("Alice", "alice@example.com"); // Only 2 fields

        assertThatThrownBy(() -> resolver.resolve(record))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "UPDATE parameter count mismatch: query expects 3 parameters (2 SET + 1 WHERE) but record has 2 fields");
    }

    @Test
    void testNullValues() {
        FixedColumnValueResolver<Row> resolver =
                new FixedColumnValueResolver<>(
                        RecordFormatType.ROW, Arrays.asList("id", "optional_field"));

        Row record = Row.of(123, null);
        ResolvedWrite result = resolver.resolve(record);

        assertThat(result.setValues()).containsExactly(123, null);
    }

    @Test
    void testDifferentRecordFormatTypes() {
        // Test that different RecordFormatType values are accepted
        FixedColumnValueResolver<Row> rowResolver =
                new FixedColumnValueResolver<>(
                        RecordFormatType.ROW, Collections.singletonList("id"));

        FixedColumnValueResolver<Tuple3<Integer, String, String>> tupleResolver =
                new FixedColumnValueResolver<>(
                        RecordFormatType.TUPLE, Collections.singletonList("id"));

        assertThat(rowResolver.kind()).isEqualTo(ColumnValueResolver.Kind.INSERT);
        assertThat(tupleResolver.kind()).isEqualTo(ColumnValueResolver.Kind.INSERT);
    }
}
