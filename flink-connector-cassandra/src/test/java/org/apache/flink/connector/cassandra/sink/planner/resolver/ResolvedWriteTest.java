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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link ResolvedWrite}. */
class ResolvedWriteTest {

    @Test
    void testInsertSingleColumn() {
        ResolvedWrite insert =
                ResolvedWrite.insert(Collections.singletonList("id"), new Object[] {123});

        assertThat(insert.isUpdate()).isFalse();
        assertThat(insert.setColumns()).containsExactly("id");
        assertThat(insert.setValues()).containsExactly(123);
        assertThat(insert.whereColumns()).isEmpty();
        assertThat(insert.whereValues()).isEmpty();
    }

    @Test
    void testInsertMultipleColumns() {
        ResolvedWrite insert =
                ResolvedWrite.insert(
                        Arrays.asList("id", "name", "email"),
                        new Object[] {101, "Alice", "alice@example.com"});

        assertThat(insert.isUpdate()).isFalse();
        assertThat(insert.setColumns()).containsExactly("id", "name", "email");
        assertThat(insert.setValues()).containsExactly(101, "Alice", "alice@example.com");
        assertThat(insert.whereColumns()).isEmpty();
        assertThat(insert.whereValues()).isEmpty();
    }

    @Test
    void testInsertNullColumns() {
        assertThatThrownBy(() -> ResolvedWrite.insert(null, new Object[] {123}))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("setColumns cannot be null");
    }

    @Test
    void testInsertNullValues() {
        assertThatThrownBy(() -> ResolvedWrite.insert(Collections.singletonList("id"), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("setValues cannot be null");
    }

    @Test
    void testInsertEmptyColumns() {
        assertThatThrownBy(() -> ResolvedWrite.insert(Collections.emptyList(), new Object[] {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("setColumns cannot be empty");
    }

    @Test
    void testInsertMismatchedCounts() {
        assertThatThrownBy(
                        () -> ResolvedWrite.insert(Arrays.asList("id", "name"), new Object[] {123}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("setColumns size (2) must equal setValues length (1)");
    }

    @Test
    void testUpdateSingleColumns() {
        ResolvedWrite update =
                ResolvedWrite.update(
                        Collections.singletonList("name"),
                        new Object[] {"Bob"},
                        Collections.singletonList("id"),
                        new Object[] {123});

        assertThat(update.isUpdate()).isTrue();
        assertThat(update.setColumns()).containsExactly("name");
        assertThat(update.setValues()).containsExactly("Bob");
        assertThat(update.whereColumns()).containsExactly("id");
        assertThat(update.whereValues()).containsExactly(123);
    }

    @Test
    void testUpdateMultipleColumns() {
        ResolvedWrite update =
                ResolvedWrite.update(
                        Arrays.asList("name", "email"),
                        new Object[] {"Bob Updated", "bob@example.com"},
                        Arrays.asList("id", "version"),
                        new Object[] {123, 1});

        assertThat(update.isUpdate()).isTrue();
        assertThat(update.setColumns()).containsExactly("name", "email");
        assertThat(update.setValues()).containsExactly("Bob Updated", "bob@example.com");
        assertThat(update.whereColumns()).containsExactly("id", "version");
        assertThat(update.whereValues()).containsExactly(123, 1);
    }

    @Test
    void testUpdateOverlappingColumns() {
        assertThatThrownBy(
                        () ->
                                ResolvedWrite.update(
                                        Arrays.asList("id", "name"),
                                        new Object[] {123, "Alice"},
                                        Arrays.asList("id", "version"),
                                        new Object[] {123, 1}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column 'id' cannot appear in both SET and WHERE clauses");
    }

    @Test
    void testUpdateMismatchedSetCounts() {
        assertThatThrownBy(
                        () ->
                                ResolvedWrite.update(
                                        Arrays.asList("name", "email"),
                                        new Object[] {"Bob"},
                                        Collections.singletonList("id"),
                                        new Object[] {123}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("setColumns size (2) must equal setValues length (1)");
    }

    @Test
    void testUpdateMismatchedWhereCounts() {
        assertThatThrownBy(
                        () ->
                                ResolvedWrite.update(
                                        Collections.singletonList("name"),
                                        new Object[] {"Bob"},
                                        Arrays.asList("id", "version"),
                                        new Object[] {123}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("whereColumns size (2) must equal whereValues length (1)");
    }

    @Test
    void testUpdateEmptySetColumns() {
        assertThatThrownBy(
                        () ->
                                ResolvedWrite.update(
                                        Collections.emptyList(),
                                        new Object[] {},
                                        Collections.singletonList("id"),
                                        new Object[] {123}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("setColumns cannot be empty");
    }

    @Test
    void testUpdateWithEmptyWhere() {
        ResolvedWrite update =
                ResolvedWrite.update(
                        Collections.singletonList("status"),
                        new Object[] {"active"},
                        Collections.emptyList(),
                        new Object[] {});

        assertThat(update.isUpdate()).isFalse(); // Empty WHERE means it's treated like INSERT
        assertThat(update.setColumns()).containsExactly("status");
        assertThat(update.setValues()).containsExactly("active");
        assertThat(update.whereColumns()).isEmpty();
        assertThat(update.whereValues()).isEmpty();
    }

    @Test
    void testNullValuesInArrays() {
        ResolvedWrite insert =
                ResolvedWrite.insert(
                        Arrays.asList("id", "optional_field"), new Object[] {123, null});

        assertThat(insert.setValues()).containsExactly(123, null);
    }
}
