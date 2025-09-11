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

import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link ColumnValueResolver} interface and its contract. */
class ColumnValueResolverTest {

    @Test
    void testInsertResolverContract() {
        ColumnValueResolver<Row> insertResolver = new TestInsertResolver();

        assertThat(insertResolver.kind()).isEqualTo(ColumnValueResolver.Kind.INSERT);

        Row record = Row.of(123, "Alice", "alice@example.com");
        ResolvedWrite result = insertResolver.resolve(record);

        assertThat(result.isUpdate()).isFalse();
        assertThat(result.setColumns()).containsExactly("id", "name", "email");
        assertThat(result.setValues()).containsExactly(123, "Alice", "alice@example.com");
        assertThat(result.whereColumns()).isEmpty();
        assertThat(result.whereValues()).isEmpty();
    }

    @Test
    void testUpdateResolverContract() {
        ColumnValueResolver<Row> updateResolver = new TestUpdateResolver();

        assertThat(updateResolver.kind()).isEqualTo(ColumnValueResolver.Kind.UPDATE);

        Row record = Row.of(123, "Alice Updated", "alice.updated@example.com");
        ResolvedWrite result = updateResolver.resolve(record);

        assertThat(result.isUpdate()).isTrue();
        assertThat(result.setColumns()).containsExactly("name", "email");
        assertThat(result.setValues())
                .containsExactly("Alice Updated", "alice.updated@example.com");
        assertThat(result.whereColumns()).containsExactly("id");
        assertThat(result.whereValues()).containsExactly(123);
    }

    @Test
    void testConditionalResolver() {
        ColumnValueResolver<Row> conditionalResolver = new TestConditionalResolver();

        assertThat(conditionalResolver.kind())
                .isEqualTo(ColumnValueResolver.Kind.INSERT); // Default/primary

        // INSERT case: update_flag is null
        Row insertRecord = Row.of(123, "Alice", "alice@example.com", null);
        ResolvedWrite insertResult = conditionalResolver.resolve(insertRecord);

        assertThat(insertResult.isUpdate()).isFalse();
        assertThat(insertResult.setColumns()).containsExactly("id", "name", "email");
        assertThat(insertResult.setValues()).containsExactly(123, "Alice", "alice@example.com");

        // UPDATE case: update_flag is not null
        Row updateRecord = Row.of(123, "Alice Updated", "alice.updated@example.com", "UPDATE");
        ResolvedWrite updateResult = conditionalResolver.resolve(updateRecord);

        assertThat(updateResult.isUpdate()).isTrue();
        assertThat(updateResult.setColumns()).containsExactly("name", "email");
        assertThat(updateResult.setValues())
                .containsExactly("Alice Updated", "alice.updated@example.com");
        assertThat(updateResult.whereColumns()).containsExactly("id");
        assertThat(updateResult.whereValues()).containsExactly(123);
    }

    // Test implementations demonstrating the interface contract

    private static class TestInsertResolver implements ColumnValueResolver<Row> {

        @Override
        public Kind kind() {
            return Kind.INSERT;
        }

        @Override
        public ResolvedWrite resolve(Row record) {
            return ResolvedWrite.insert(
                    Arrays.asList("id", "name", "email"),
                    new Object[] {
                        record.getField(0), // id
                        record.getField(1), // name
                        record.getField(2) // email
                    });
        }
    }

    private static class TestUpdateResolver implements ColumnValueResolver<Row> {

        @Override
        public Kind kind() {
            return Kind.UPDATE;
        }

        @Override
        public ResolvedWrite resolve(Row record) {
            return ResolvedWrite.update(
                    Arrays.asList("name", "email"), // SET columns
                    new Object[] {
                        record.getField(1), // name
                        record.getField(2) // email
                    },
                    Collections.singletonList("id"), // WHERE columns
                    new Object[] {
                        record.getField(0) // id
                    });
        }
    }

    private static class TestConditionalResolver implements ColumnValueResolver<Row> {

        @Override
        public Kind kind() {
            // Return primary/default kind - this resolver can handle both
            return Kind.INSERT;
        }

        @Override
        public ResolvedWrite resolve(Row record) {
            boolean isUpdate = record.getField(3) != null; // has update flag

            if (isUpdate) {
                return ResolvedWrite.update(
                        Arrays.asList("name", "email"),
                        new Object[] {record.getField(1), record.getField(2)},
                        Collections.singletonList("id"),
                        new Object[] {record.getField(0)});
            } else {
                return ResolvedWrite.insert(
                        Arrays.asList("id", "name", "email"),
                        new Object[] {record.getField(0), record.getField(1), record.getField(2)});
            }
        }
    }
}
