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

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import com.datastax.driver.core.UDTValue;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for {@link CassandraFieldMapperFactory}. */
class CassandraFieldMapperFactoryTest {

    @Test
    void testPrimitiveTypeMappers() {
        CassandraFieldMapper booleanMapper =
                CassandraFieldMapperFactory.createFieldMapper(new BooleanType());
        assertThat(booleanMapper)
                .isNotNull()
                .isInstanceOf(PrimitiveFieldMappers.BooleanMapper.class);

        CassandraFieldMapper byteMapper =
                CassandraFieldMapperFactory.createFieldMapper(new TinyIntType());
        assertThat(byteMapper).isNotNull().isInstanceOf(PrimitiveFieldMappers.ByteMapper.class);

        CassandraFieldMapper shortMapper =
                CassandraFieldMapperFactory.createFieldMapper(new SmallIntType());
        assertThat(shortMapper).isNotNull().isInstanceOf(PrimitiveFieldMappers.ShortMapper.class);

        CassandraFieldMapper intMapper =
                CassandraFieldMapperFactory.createFieldMapper(new IntType());
        assertThat(intMapper).isNotNull().isInstanceOf(PrimitiveFieldMappers.IntegerMapper.class);

        CassandraFieldMapper longMapper =
                CassandraFieldMapperFactory.createFieldMapper(new BigIntType());
        assertThat(longMapper).isNotNull().isInstanceOf(PrimitiveFieldMappers.LongMapper.class);

        CassandraFieldMapper floatMapper =
                CassandraFieldMapperFactory.createFieldMapper(new FloatType());
        assertThat(floatMapper).isNotNull().isInstanceOf(PrimitiveFieldMappers.FloatMapper.class);

        CassandraFieldMapper doubleMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DoubleType());
        assertThat(doubleMapper).isNotNull().isInstanceOf(PrimitiveFieldMappers.DoubleMapper.class);

        CassandraFieldMapper stringMapper =
                CassandraFieldMapperFactory.createFieldMapper(VarCharType.STRING_TYPE);
        assertThat(stringMapper).isNotNull().isInstanceOf(PrimitiveFieldMappers.StringMapper.class);

        CassandraFieldMapper charMapper =
                CassandraFieldMapperFactory.createFieldMapper(new CharType(10));
        assertThat(charMapper).isNotNull().isInstanceOf(PrimitiveFieldMappers.StringMapper.class);

        CassandraFieldMapper dateMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DateType());
        assertThat(dateMapper).isNotNull().isInstanceOf(PrimitiveFieldMappers.DateMapper.class);

        CassandraFieldMapper timeMapper =
                CassandraFieldMapperFactory.createFieldMapper(new TimeType());
        assertThat(timeMapper).isNotNull().isInstanceOf(PrimitiveFieldMappers.TimeMapper.class);

        CassandraFieldMapper timestampMapper =
                CassandraFieldMapperFactory.createFieldMapper(new TimestampType());
        assertThat(timestampMapper)
                .isNotNull()
                .isInstanceOf(PrimitiveFieldMappers.TimestampMapper.class);

        CassandraFieldMapper binaryMapper =
                CassandraFieldMapperFactory.createFieldMapper(new BinaryType(10));
        assertThat(binaryMapper).isNotNull().isInstanceOf(PrimitiveFieldMappers.BinaryMapper.class);

        CassandraFieldMapper varbinaryMapper =
                CassandraFieldMapperFactory.createFieldMapper(new VarBinaryType(100));
        assertThat(varbinaryMapper)
                .isNotNull()
                .isInstanceOf(PrimitiveFieldMappers.BinaryMapper.class);
    }

    @Test
    void testDecimalMappers() {
        // Test that all decimal types now use DynamicDecimalMapper
        // This mapper determines the actual Cassandra type (varint vs decimal) at runtime

        // Test regular decimal precision
        CassandraFieldMapper decimalMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DecimalType(10, 2));
        assertThat(decimalMapper)
                .isNotNull()
                .isInstanceOf(PrimitiveFieldMappers.DynamicDecimalMapper.class);

        // Test high precision decimal
        CassandraFieldMapper highPrecisionMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DecimalType(25, 5));
        assertThat(highPrecisionMapper)
                .isNotNull()
                .isInstanceOf(PrimitiveFieldMappers.DynamicDecimalMapper.class);

        // Test edge case: exactly 18 precision
        CassandraFieldMapper edgeDecimalMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DecimalType(18, 2));
        assertThat(edgeDecimalMapper)
                .isNotNull()
                .isInstanceOf(PrimitiveFieldMappers.DynamicDecimalMapper.class);

        // Test edge case: 19 precision
        CassandraFieldMapper edgeVarintMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DecimalType(19, 2));
        assertThat(edgeVarintMapper)
                .isNotNull()
                .isInstanceOf(PrimitiveFieldMappers.DynamicDecimalMapper.class);
    }

    @Test
    void testArrayMapper() {
        ArrayType stringArrayType = new ArrayType(VarCharType.STRING_TYPE);
        CassandraFieldMapper arrayMapper =
                CassandraFieldMapperFactory.createFieldMapper(stringArrayType);
        assertThat(arrayMapper).isNotNull().isInstanceOf(CollectionFieldMappers.ArrayMapper.class);

        ArrayType nestedArrayType = new ArrayType(new ArrayType(new IntType()));
        CassandraFieldMapper nestedArrayMapper =
                CassandraFieldMapperFactory.createFieldMapper(nestedArrayType);
        assertThat(nestedArrayMapper)
                .isNotNull()
                .isInstanceOf(CollectionFieldMappers.ArrayMapper.class);
        // Test conversion of nested array structure (this exercises the inner mappers)
        List<List<Integer>> testData =
                Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6));
        Object result = nestedArrayMapper.convertValueToFlinkType(testData);
        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(GenericArrayData.class);
    }

    @Test
    void testMapMapper() {
        MapType mapType = new MapType(VarCharType.STRING_TYPE, new IntType());
        CassandraFieldMapper mapMapper = CassandraFieldMapperFactory.createFieldMapper(mapType);
        assertThat(mapMapper).isNotNull().isInstanceOf(CollectionFieldMappers.MapMapper.class);

        MapType nestedMapType =
                new MapType(
                        VarCharType.STRING_TYPE,
                        new MapType(VarCharType.STRING_TYPE, new IntType()));
        CassandraFieldMapper nestedMapMapper =
                CassandraFieldMapperFactory.createFieldMapper(nestedMapType);
        assertThat(nestedMapMapper)
                .isNotNull()
                .isInstanceOf(CollectionFieldMappers.MapMapper.class);

        // Test that nested map creation works correctly, verifying inner mappers
        // Test conversion of nested map structure (this exercises the inner mappers)
        Map<String, Map<String, Integer>> testData = new HashMap<>();
        Map<String, Integer> innerMap = new HashMap<>();
        innerMap.put("score", 95);
        innerMap.put("grade", 85);
        testData.put("user1", innerMap);

        Object result = nestedMapMapper.convertValueToFlinkType(testData);
        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(GenericMapData.class);
    }

    @Test
    void testMultisetMapper() {
        MultisetType multisetType = new MultisetType(VarCharType.STRING_TYPE);
        CassandraFieldMapper setMapper =
                CassandraFieldMapperFactory.createFieldMapper(multisetType);
        assertThat(setMapper).isNotNull().isInstanceOf(CollectionFieldMappers.SetMapper.class);

        MultisetType nestedMultisetType = new MultisetType(new ArrayType(new IntType()));
        CassandraFieldMapper nestedSetMapper =
                CassandraFieldMapperFactory.createFieldMapper(nestedMultisetType);
        assertThat(nestedSetMapper)
                .isNotNull()
                .isInstanceOf(CollectionFieldMappers.SetMapper.class);

        // Test that nested multiset creation works correctly, verifying inner mappers
        Set<List<Integer>> testData = new HashSet<>();
        testData.add(Arrays.asList(1, 2, 3));
        testData.add(Arrays.asList(4, 5, 6));

        Object result = nestedSetMapper.convertValueToFlinkType(testData);
        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(GenericMapData.class);
    }

    @Test
    void testRowMapper() {
        RowType rowType =
                RowType.of(
                        new LogicalType[] {
                            VarCharType.STRING_TYPE, new IntType(), new BooleanType()
                        },
                        new String[] {"name", "age", "active"});
        CassandraFieldMapper rowMapper = CassandraFieldMapperFactory.createFieldMapper(rowType);
        assertThat(rowMapper).isNotNull().isInstanceOf(CollectionFieldMappers.RowMapper.class);

        RowType nestedRowType =
                RowType.of(
                        new LogicalType[] {
                            VarCharType.STRING_TYPE,
                            rowType, // nested row
                            new ArrayType(new IntType())
                        },
                        new String[] {"id", "user_info", "scores"});
        CassandraFieldMapper nestedRowMapper =
                CassandraFieldMapperFactory.createFieldMapper(nestedRowType);
        assertThat(nestedRowMapper)
                .isNotNull()
                .isInstanceOf(CollectionFieldMappers.RowMapper.class);

        // Test that nested row creation works correctly by testing conversion
        // Create mock UDT data that would exercise the nested row mappers
        // The fact that this doesn't throw an exception means the inner mappers exist
        UDTValue mockUDT = mock(UDTValue.class);
        UDTValue innerMockUDT = mock(UDTValue.class);

        // Mock the nested UDT structure
        when(mockUDT.isNull("id")).thenReturn(false);
        when(mockUDT.getObject("id")).thenReturn("test_id");
        when(mockUDT.isNull("user_info")).thenReturn(false);
        when(mockUDT.getObject("user_info")).thenReturn(innerMockUDT);
        when(mockUDT.isNull("scores")).thenReturn(false);
        when(mockUDT.getObject("scores")).thenReturn(Arrays.asList(1, 2, 3));

        when(innerMockUDT.isNull("name")).thenReturn(false);
        when(innerMockUDT.getObject("name")).thenReturn("John");
        when(innerMockUDT.isNull("age")).thenReturn(false);
        when(innerMockUDT.getObject("age")).thenReturn(30);
        when(innerMockUDT.isNull("active")).thenReturn(false);
        when(innerMockUDT.getObject("active")).thenReturn(true);

        Object result = nestedRowMapper.convertValueToFlinkType(mockUDT);
        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(GenericRowData.class);
    }

    @Test
    void testComplexNestedStructures() {
        // Test deeply nested structure: array<map<string, row<name:string, scores:array<int>>>>
        RowType innerRowType =
                RowType.of(
                        new LogicalType[] {VarCharType.STRING_TYPE, new ArrayType(new IntType())},
                        new String[] {"name", "scores"});
        MapType mapType = new MapType(VarCharType.STRING_TYPE, innerRowType);
        ArrayType complexArrayType = new ArrayType(mapType);

        CassandraFieldMapper complexMapper =
                CassandraFieldMapperFactory.createFieldMapper(complexArrayType);
        assertThat(complexMapper)
                .isNotNull()
                .isInstanceOf(CollectionFieldMappers.ArrayMapper.class);

        RowType complexRowType =
                RowType.of(
                        new LogicalType[] {
                            new IntType(),
                            new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE),
                            new MultisetType(VarCharType.STRING_TYPE)
                        },
                        new String[] {"id", "metadata", "tags"});

        CassandraFieldMapper complexRowMapper =
                CassandraFieldMapperFactory.createFieldMapper(complexRowType);
        assertThat(complexRowMapper)
                .isNotNull()
                .isInstanceOf(CollectionFieldMappers.RowMapper.class);

        assertThat(complexMapper).isNotNull();
        assertThat(complexRowMapper).isNotNull();

        // Additional verification: Test that simpler conversions work to prove mappers function
        // Test simple map conversion (string -> string)
        Map<String, String> simpleMap = new HashMap<>();
        simpleMap.put("key1", "value1");

        // Get a simple map mapper to test
        MapType simpleMapType = new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE);
        CassandraFieldMapper simpleMapMapper =
                CassandraFieldMapperFactory.createFieldMapper(simpleMapType);
        Object mapResult = simpleMapMapper.convertValueToFlinkType(simpleMap);
        assertThat(mapResult).isNotNull();
        assertThat(mapResult).isInstanceOf(GenericMapData.class);
    }

    @Test
    void testInternalMappersInNestedTypes() {
        // Test that internal element mappers are correctly created for nested types

        // Test Array with StringMapper element
        ArrayType stringArrayType = new ArrayType(VarCharType.STRING_TYPE);
        CollectionFieldMappers.ArrayMapper arrayMapper =
                (CollectionFieldMappers.ArrayMapper)
                        CassandraFieldMapperFactory.createFieldMapper(stringArrayType);
        assertThat(arrayMapper).isNotNull();
        // Verify the element mapper is a StringMapper (testing internal mapper)
        // We can't directly access the element mapper due to private field, but we can test it
        // works
        List<String> testStrings = Arrays.asList("hello", "world");
        Object result = arrayMapper.convertValueToFlinkType(testStrings);
        assertThat(result).isNotNull().isInstanceOf(GenericArrayData.class);

        // Test Map with String key mapper and Integer value mapper
        MapType mapType = new MapType(VarCharType.STRING_TYPE, new IntType());
        CollectionFieldMappers.MapMapper mapMapper =
                (CollectionFieldMappers.MapMapper)
                        CassandraFieldMapperFactory.createFieldMapper(mapType);
        assertThat(mapMapper).isNotNull();
        // Test that internal key and value mappers work correctly
        Map<String, Integer> testMap = new HashMap<>();
        testMap.put("key1", 100);
        testMap.put("key2", 200);
        Object mapResult = mapMapper.convertValueToFlinkType(testMap);
        assertThat(mapResult).isNotNull().isInstanceOf(GenericMapData.class);

        // Test nested Array<Array<Int>> to verify both outer and inner ArrayMappers work
        ArrayType nestedArrayType = new ArrayType(new ArrayType(new IntType()));
        CollectionFieldMappers.ArrayMapper nestedArrayMapper =
                (CollectionFieldMappers.ArrayMapper)
                        CassandraFieldMapperFactory.createFieldMapper(nestedArrayType);
        assertThat(nestedArrayMapper).isNotNull();
        // Test that both outer and inner array mappers function correctly
        List<List<Integer>> nestedData =
                Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6));
        Object nestedResult = nestedArrayMapper.convertValueToFlinkType(nestedData);
        assertThat(nestedResult).isNotNull().isInstanceOf(GenericArrayData.class);

        // Test Multiset with complex element type (Array<String>)
        MultisetType multisetWithArrayType =
                new MultisetType(new ArrayType(VarCharType.STRING_TYPE));
        CollectionFieldMappers.SetMapper setMapper =
                (CollectionFieldMappers.SetMapper)
                        CassandraFieldMapperFactory.createFieldMapper(multisetWithArrayType);
        assertThat(setMapper).isNotNull();
        // Test that the set mapper with array element mapper works
        Set<List<String>> setData = new HashSet<>();
        setData.add(Arrays.asList("a", "b"));
        setData.add(Arrays.asList("c", "d"));
        Object setResult = setMapper.convertValueToFlinkType(setData);
        assertThat(setResult).isNotNull().isInstanceOf(GenericMapData.class);

        // Test Row with multiple field mappers of different types
        RowType rowType =
                RowType.of(
                        new LogicalType[] {
                            new IntType(), VarCharType.STRING_TYPE, new BooleanType()
                        },
                        new String[] {"id", "name", "active"});
        CollectionFieldMappers.RowMapper rowMapper =
                (CollectionFieldMappers.RowMapper)
                        CassandraFieldMapperFactory.createFieldMapper(rowType);
        assertThat(rowMapper).isNotNull();
        // Create a mock UDT to test that all field mappers work
        UDTValue mockUDT = mock(UDTValue.class);
        when(mockUDT.isNull("id")).thenReturn(false);
        when(mockUDT.getObject("id")).thenReturn(123);
        when(mockUDT.isNull("name")).thenReturn(false);
        when(mockUDT.getObject("name")).thenReturn("TestName");
        when(mockUDT.isNull("active")).thenReturn(false);
        when(mockUDT.getObject("active")).thenReturn(true);
        Object rowResult = rowMapper.convertValueToFlinkType(mockUDT);
        assertThat(rowResult).isNotNull().isInstanceOf(GenericRowData.class);
    }

    @Test
    void testRecursiveMapperCreation() {
        // Test array<array<map<string, row<id:int, values:multiset<double>>>>>
        RowType deepRowType =
                RowType.of(
                        new LogicalType[] {new IntType(), new MultisetType(new DoubleType())},
                        new String[] {"id", "values"});
        MapType deepMapType = new MapType(VarCharType.STRING_TYPE, deepRowType);
        ArrayType deepArrayType = new ArrayType(new ArrayType(deepMapType));

        CassandraFieldMapper deepMapper =
                CassandraFieldMapperFactory.createFieldMapper(deepArrayType);
        assertThat(deepMapper).isNotNull().isInstanceOf(CollectionFieldMappers.ArrayMapper.class);
        // Test that the deeply nested structure can be created without exceptions
        // This is the key test - if inner mappers aren't created properly, the factory would fail
        assertThat(deepMapper).isNotNull();

        // Test that a simple nested array works (array<array<string>>)
        ArrayType simpleNestedArray = new ArrayType(new ArrayType(VarCharType.STRING_TYPE));
        CassandraFieldMapper simpleNestedMapper =
                CassandraFieldMapperFactory.createFieldMapper(simpleNestedArray);

        List<List<String>> testData =
                Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("c", "d"));

        Object result = simpleNestedMapper.convertValueToFlinkType(testData);
        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(GenericArrayData.class);
    }
}
