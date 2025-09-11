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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowDataFieldsResolver}. */
class RowDataFieldsResolverTest {

    // Test 1: Basic routing
    @Test
    void testInsertPath() {
        // Given: RowType with fields [id, name, age]
        RowType rowType =
                createRowType(
                        new String[] {"id", "name", "age"},
                        new LogicalType[] {new IntType(), new VarCharType(), new IntType()});

        // When: No WHERE columns (INSERT path)
        List<String> setColumns = Arrays.asList("id", "name", "age");
        RowDataFieldsResolver resolver = new RowDataFieldsResolver(rowType, setColumns);

        // Then: kind() returns INSERT
        assertThat(resolver.kind()).isEqualTo(ColumnValueResolver.Kind.INSERT);

        // And: resolve() returns ResolvedWrite.insert with values in setColumns order
        GenericRowData row = GenericRowData.of(1, StringData.fromString("Alice"), 30);
        ResolvedWrite resolved = resolver.resolve(row);

        assertThat(resolved.setColumns()).isEqualTo(setColumns);
        assertThat(resolved.setValues()).containsExactly(1, StringData.fromString("Alice"), 30);
        assertThat(resolved.whereColumns()).isEmpty();
        assertThat(resolved.whereValues()).isEmpty();
    }

    @Test
    void testUpdatePath() {
        // Given: RowType with fields [id, name, age]
        RowType rowType =
                createRowType(
                        new String[] {"id", "name", "age"},
                        new LogicalType[] {new IntType(), new VarCharType(), new IntType()});

        // When: WHERE columns present (UPDATE path)
        List<String> setColumns = Arrays.asList("name", "age");
        List<String> whereColumns = Arrays.asList("id");
        RowDataFieldsResolver resolver =
                new RowDataFieldsResolver(rowType, setColumns, whereColumns);

        // Then: kind() returns UPDATE
        assertThat(resolver.kind()).isEqualTo(ColumnValueResolver.Kind.UPDATE);

        // And: resolve() returns ResolvedWrite.update with SET first, then WHERE
        GenericRowData row = GenericRowData.of(1, StringData.fromString("Bob"), 25);
        ResolvedWrite resolved = resolver.resolve(row);

        assertThat(resolved.setColumns()).isEqualTo(setColumns);
        assertThat(resolved.setValues()).containsExactly(StringData.fromString("Bob"), 25);
        assertThat(resolved.whereColumns()).isEqualTo(whereColumns);
        assertThat(resolved.whereValues()).containsExactly(1);
    }

    // Test 2: Column → index mapping & ordering
    @Test
    void testNonContiguousIndicesAndOrdering() {
        // Given: RowType with fields [id, a, b, c]
        RowType rowType =
                createRowType(
                        new String[] {"id", "a", "b", "c"},
                        new LogicalType[] {
                            new IntType(), new VarCharType(), new DoubleType(), new BooleanType()
                        });

        // When: Non-contiguous SET columns [c, a] and WHERE column [id]
        List<String> setColumns = Arrays.asList("c", "a"); // Indices 3, 1
        List<String> whereColumns = Arrays.asList("id"); // Index 0
        RowDataFieldsResolver resolver =
                new RowDataFieldsResolver(rowType, setColumns, whereColumns);

        // Then: Values are extracted in the exact order specified
        GenericRowData row = GenericRowData.of(100, StringData.fromString("alpha"), 3.14, true);
        ResolvedWrite resolved = resolver.resolve(row);

        // Values should be [row.c, row.a] then [row.id]
        assertThat(resolved.setValues()).containsExactly(true, StringData.fromString("alpha"));
        assertThat(resolved.whereValues()).containsExactly(100);
    }

    @Test
    void testOrderPreservation() {
        // Given: RowType with fields [f1, f2, f3, f4]
        RowType rowType =
                createRowType(
                        new String[] {"f1", "f2", "f3", "f4"},
                        new LogicalType[] {
                            new IntType(), new IntType(), new IntType(), new IntType()
                        });

        // When: SET columns in shuffled order [f3, f1, f4, f2]
        List<String> setColumns = Arrays.asList("f3", "f1", "f4", "f2");
        RowDataFieldsResolver resolver = new RowDataFieldsResolver(rowType, setColumns);

        // Then: Values follow the shuffled order, not schema order
        GenericRowData row = GenericRowData.of(10, 20, 30, 40);
        ResolvedWrite resolved = resolver.resolve(row);

        assertThat(resolved.setValues()).containsExactly(30, 10, 40, 20);
    }

    // Test 3: Type extraction & nulls
    @Test
    void testMixedTypesAndNulls() {
        // Given: RowType with various types
        RowType rowType =
                createRowType(
                        new String[] {
                            "int_col",
                            "bigint_col",
                            "bool_col",
                            "double_col",
                            "string_col",
                            "bytes_col",
                            "timestamp_col"
                        },
                        new LogicalType[] {
                            new IntType(),
                            new BigIntType(),
                            new BooleanType(),
                            new DoubleType(),
                            new VarCharType(),
                            new BinaryType(),
                            new TimestampType(3)
                        });

        // When: Resolver with all columns
        List<String> setColumns =
                Arrays.asList(
                        "int_col",
                        "bigint_col",
                        "bool_col",
                        "double_col",
                        "string_col",
                        "bytes_col",
                        "timestamp_col");
        RowDataFieldsResolver resolver = new RowDataFieldsResolver(rowType, setColumns);

        // Then: Correct Java types extracted including nulls
        byte[] bytes = new byte[] {1, 2, 3};
        TimestampData ts = TimestampData.fromInstant(Instant.parse("2024-01-01T00:00:00Z"));

        GenericRowData row =
                GenericRowData.of(
                        42, // Integer
                        123456789L, // Long
                        true, // Boolean
                        3.14159, // Double
                        StringData.fromString("hello"), // String
                        bytes, // byte[]
                        ts // Timestamp
                        );

        ResolvedWrite resolved = resolver.resolve(row);
        Object[] values = resolved.setValues();

        assertThat(values[0]).isEqualTo(42);
        assertThat(values[1]).isEqualTo(123456789L);
        assertThat(values[2]).isEqualTo(true);
        assertThat(values[3]).isEqualTo(3.14159);
        assertThat(values[4]).isEqualTo(StringData.fromString("hello"));
        assertThat(values[5]).isEqualTo(bytes);
        assertThat(values[6]).isEqualTo(ts);

        // Test with null values
        GenericRowData nullRow =
                GenericRowData.of(null, 123456789L, null, 3.14159, null, bytes, null);

        ResolvedWrite nullResolved = resolver.resolve(nullRow);
        Object[] nullValues = nullResolved.setValues();

        assertThat(nullValues[0]).isNull();
        assertThat(nullValues[1]).isEqualTo(123456789L);
        assertThat(nullValues[2]).isNull();
        assertThat(nullValues[3]).isEqualTo(3.14159);
        assertThat(nullValues[4]).isNull();
        assertThat(nullValues[5]).isEqualTo(bytes);
        assertThat(nullValues[6]).isNull();
    }

    // Test 4: Validation errors
    @Test
    void testMissingSetColumnError() {
        RowType rowType =
                createRowType(
                        new String[] {"id", "name"},
                        new LogicalType[] {new IntType(), new VarCharType()});

        List<String> setColumns = Arrays.asList("name", "missing");

        assertThatThrownBy(() -> new RowDataFieldsResolver(rowType, setColumns))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SET column 'missing' not found in RowType schema");
    }

    @Test
    void testMissingWhereColumnError() {
        RowType rowType =
                createRowType(
                        new String[] {"id", "name"},
                        new LogicalType[] {new IntType(), new VarCharType()});

        List<String> setColumns = Collections.singletonList("name");
        List<String> whereColumns = Collections.singletonList("missing");

        assertThatThrownBy(() -> new RowDataFieldsResolver(rowType, setColumns, whereColumns))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("WHERE column 'missing' not found in RowType schema");
    }

    @Test
    void testDuplicateInSetError() {
        RowType rowType =
                createRowType(
                        new String[] {"id", "name", "age"},
                        new LogicalType[] {new IntType(), new VarCharType(), new IntType()});

        List<String> setColumns = Arrays.asList("name", "age", "name");

        assertThatThrownBy(() -> new RowDataFieldsResolver(rowType, setColumns))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate column in SET list: 'name'");
    }

    @Test
    void testOverlapSetAndWhereError() {
        RowType rowType =
                createRowType(
                        new String[] {"id", "name", "age"},
                        new LogicalType[] {new IntType(), new VarCharType(), new IntType()});

        List<String> setColumns = Arrays.asList("id", "name");
        List<String> whereColumns = Collections.singletonList("id");

        assertThatThrownBy(() -> new RowDataFieldsResolver(rowType, setColumns, whereColumns))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column appears in both SET and WHERE: 'id'");
    }

    @Test
    void testUpdateWithNoSetError() {
        RowType rowType =
                createRowType(
                        new String[] {"id", "name"},
                        new LogicalType[] {new IntType(), new VarCharType()});

        List<String> setColumns = Collections.emptyList();
        List<String> whereColumns = Arrays.asList("id");

        assertThatThrownBy(() -> new RowDataFieldsResolver(rowType, setColumns, whereColumns))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UPDATE requires at least one SET column");
    }

    // Test 5: Edge cases
    @Test
    void testMinimalInsert() {
        // Given: Single column RowType
        RowType rowType = createRowType(new String[] {"id"}, new LogicalType[] {new IntType()});

        // When: Single SET column (minimal valid INSERT)
        List<String> setColumns = Collections.singletonList("id");
        RowDataFieldsResolver resolver = new RowDataFieldsResolver(rowType, setColumns);

        // Then: Should be allowed as INSERT
        assertThat(resolver.kind()).isEqualTo(ColumnValueResolver.Kind.INSERT);

        GenericRowData row = GenericRowData.of(42);
        ResolvedWrite resolved = resolver.resolve(row);

        assertThat(resolved.setColumns()).containsExactly("id");
        assertThat(resolved.setValues()).containsExactly(42);
        assertThat(resolved.whereColumns()).isEmpty();
        assertThat(resolved.whereValues()).isEmpty();
    }

    @Test
    void testSingleWhereMultiSet() {
        // Given: RowType with 4 fields
        RowType rowType =
                createRowType(
                        new String[] {"id", "a", "b", "c"},
                        new LogicalType[] {
                            new IntType(), new VarCharType(), new IntType(), new DoubleType()
                        });

        // When: Multiple SET columns and single WHERE
        List<String> setColumns = Arrays.asList("a", "b", "c");
        List<String> whereColumns = Collections.singletonList("id");
        RowDataFieldsResolver resolver =
                new RowDataFieldsResolver(rowType, setColumns, whereColumns);

        // Then: Correct ordering and lengths
        GenericRowData row = GenericRowData.of(99, StringData.fromString("test"), 42, 3.14);
        ResolvedWrite resolved = resolver.resolve(row);

        assertThat(resolved.setValues()).hasSize(3);
        assertThat(resolved.setValues()).containsExactly(StringData.fromString("test"), 42, 3.14);
        assertThat(resolved.whereValues()).hasSize(1);
        assertThat(resolved.whereValues()).containsExactly(99);
    }

    @Test
    void testCaseSensitivity() {
        // Given: RowType with specific casing
        RowType rowType =
                createRowType(
                        new String[] {"userId", "userName"},
                        new LogicalType[] {new IntType(), new VarCharType()});

        // When: Incorrect casing used
        List<String> setColumns = Arrays.asList("userid"); // Wrong case

        // Then: Should fail (case-sensitive)
        assertThatThrownBy(() -> new RowDataFieldsResolver(rowType, setColumns))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SET column 'userid' not found in RowType schema");
    }

    // Test 6: Serialization
    @Test
    void testSerialization() throws Exception {
        // Given: A configured resolver
        RowType rowType =
                createRowType(
                        new String[] {"id", "name", "value"},
                        new LogicalType[] {new IntType(), new VarCharType(), new DoubleType()});

        List<String> setColumns = Arrays.asList("name", "value");
        List<String> whereColumns = Collections.singletonList("id");
        RowDataFieldsResolver original =
                new RowDataFieldsResolver(rowType, setColumns, whereColumns);

        // When: Serialized and deserialized
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(original);
        oos.close();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        RowDataFieldsResolver deserialized = (RowDataFieldsResolver) ois.readObject();
        ois.close();

        // Then: Deserialized resolver should work correctly
        assertThat(deserialized.kind()).isEqualTo(ColumnValueResolver.Kind.UPDATE);

        GenericRowData row = GenericRowData.of(123, StringData.fromString("test"), 99.9);
        ResolvedWrite resolved = deserialized.resolve(row);

        assertThat(resolved.setValues()).containsExactly(StringData.fromString("test"), 99.9);
        assertThat(resolved.whereValues()).containsExactly(123);
    }

    // Additional test: Complex multi-column WHERE
    @Test
    void testMultiColumnWhere() {
        // Given: Composite primary key scenario
        RowType rowType =
                createRowType(
                        new String[] {"partition_key", "clustering_key", "value1", "value2"},
                        new LogicalType[] {
                            new IntType(), new VarCharType(), new IntType(), new BooleanType()
                        });

        // When: Multiple WHERE columns
        List<String> setColumns = Arrays.asList("value1", "value2");
        List<String> whereColumns = Arrays.asList("partition_key", "clustering_key");
        RowDataFieldsResolver resolver =
                new RowDataFieldsResolver(rowType, setColumns, whereColumns);

        // Then: Both WHERE values extracted in order
        GenericRowData row = GenericRowData.of(1, StringData.fromString("key"), 100, false);
        ResolvedWrite resolved = resolver.resolve(row);

        assertThat(resolved.setValues()).containsExactly(100, false);
        assertThat(resolved.whereValues()).containsExactly(1, StringData.fromString("key"));
    }

    // Helper method to create RowType
    private static RowType createRowType(String[] fieldNames, LogicalType[] fieldTypes) {
        return RowType.of(fieldTypes, fieldNames);
    }
}
