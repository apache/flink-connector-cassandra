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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/** Unit tests for {@link CollectionFieldMappers}. */
class CollectionFieldMappersTest {

    @Mock private Row mockRow;
    @Mock private TupleValue mockTupleValue;
    @Mock private UDTValue mockUDTValue;
    @Mock private CassandraFieldMapper mockElementMapper;
    @Mock private CassandraFieldMapper mockKeyMapper;
    @Mock private CassandraFieldMapper mockValueMapper;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testArrayMapper() {
        CollectionFieldMappers.ArrayMapper mapper =
                new CollectionFieldMappers.ArrayMapper(mockElementMapper);

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractRowFieldValue(mockRow, "field")).isNull();

        // Test array value
        when(mockRow.isNull("field")).thenReturn(false);
        List<Object> testList = Arrays.asList("a", "b", "c");
        when(mockRow.getObject("field")).thenReturn(testList);
        when(mockElementMapper.convertValueToFlinkType("a")).thenReturn("converted_a");
        when(mockElementMapper.convertValueToFlinkType("b")).thenReturn("converted_b");
        when(mockElementMapper.convertValueToFlinkType("c")).thenReturn("converted_c");

        Object result = mapper.extractRowFieldValue(mockRow, "field");
        assertThat(result).isInstanceOf(GenericArrayData.class);
        GenericArrayData resultArray = (GenericArrayData) result;
        assertThat(resultArray.size()).isEqualTo(3);
        Object[] array = resultArray.toObjectArray();
        assertThat(array).containsExactly("converted_a", "converted_b", "converted_c");

        // Test convertValue method for null
        assertThat(mapper.convertValueToFlinkType(null)).isNull();
    }

    @Test
    void testMapMapper() {
        CollectionFieldMappers.MapMapper mapper =
                new CollectionFieldMappers.MapMapper(mockKeyMapper, mockValueMapper);

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractRowFieldValue(mockRow, "field")).isNull();

        // Test map value
        when(mockRow.isNull("field")).thenReturn(false);
        Map<Object, Object> testMap = new HashMap<>();
        testMap.put("key1", 10);
        testMap.put("key2", 20);
        when(mockRow.getObject("field")).thenReturn(testMap);
        when(mockKeyMapper.convertValueToFlinkType("key1")).thenReturn("converted_key1");
        when(mockKeyMapper.convertValueToFlinkType("key2")).thenReturn("converted_key2");
        when(mockValueMapper.convertValueToFlinkType(10)).thenReturn(100);
        when(mockValueMapper.convertValueToFlinkType(20)).thenReturn(200);

        Object result = mapper.extractRowFieldValue(mockRow, "field");
        assertThat(result).isInstanceOf(GenericMapData.class);
        GenericMapData resultMap = (GenericMapData) result;
        assertThat(resultMap.size()).isEqualTo(2);

        // Verify actual key-value pairs were converted properly
        ArrayData keys = resultMap.keyArray();
        ArrayData values = resultMap.valueArray();
        assertThat(keys.size()).isEqualTo(2);
        assertThat(values.size()).isEqualTo(2);

        // Verify the actual converted content is present
        assertThat(resultMap.get("converted_key1")).isEqualTo(100);
        assertThat(resultMap.get("converted_key2")).isEqualTo(200);

        // Test convertValue method for null
        assertThat(mapper.convertValueToFlinkType(null)).isNull();
    }

    @Test
    void testSetMapper() {
        CollectionFieldMappers.SetMapper mapper =
                new CollectionFieldMappers.SetMapper(mockElementMapper);

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractRowFieldValue(mockRow, "field")).isNull();

        // Test set value
        when(mockRow.isNull("field")).thenReturn(false);
        Set<Object> testSet = new HashSet<>(Arrays.asList("x", "y", "z"));
        when(mockRow.getObject("field")).thenReturn(testSet);
        when(mockElementMapper.convertValueToFlinkType("x")).thenReturn("converted_x");
        when(mockElementMapper.convertValueToFlinkType("y")).thenReturn("converted_y");
        when(mockElementMapper.convertValueToFlinkType("z")).thenReturn("converted_z");

        Object result = mapper.extractRowFieldValue(mockRow, "field");
        assertThat(result).isInstanceOf(GenericMapData.class);
        GenericMapData resultMap = (GenericMapData) result;
        assertThat(resultMap.size()).isEqualTo(3);

        // Note: Set order is not guaranteed, so we check that the converted values are present
        // Each element should have count 1 in the multiset
        assertThat(resultMap.get("converted_x")).isEqualTo(1);
        assertThat(resultMap.get("converted_y")).isEqualTo(1);
        assertThat(resultMap.get("converted_z")).isEqualTo(1);

        // Test convertValue method for null
        assertThat(mapper.convertValueToFlinkType(null)).isNull();
    }

    @Test
    void testRowMapper() {
        CassandraFieldMapper[] fieldMappers = {mockElementMapper, mockValueMapper};
        String[] fieldNames = {"name", "age"};
        CollectionFieldMappers.RowMapper mapper =
                new CollectionFieldMappers.RowMapper(fieldMappers, fieldNames);

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractRowFieldValue(mockRow, "field")).isNull();

        // Test UDT value
        when(mockRow.isNull("field")).thenReturn(false);
        when(mockRow.getObject("field")).thenReturn(mockUDTValue);
        when(mockUDTValue.isNull("name")).thenReturn(false);
        when(mockUDTValue.isNull("age")).thenReturn(false);
        when(mockUDTValue.getObject("name")).thenReturn("John");
        when(mockUDTValue.getObject("age")).thenReturn(30);
        when(mockElementMapper.convertValueToFlinkType("John")).thenReturn("converted_John");
        when(mockValueMapper.convertValueToFlinkType(30)).thenReturn(300);

        Object result = mapper.extractRowFieldValue(mockRow, "field");
        assertThat(result).isInstanceOf(GenericRowData.class);
        GenericRowData resultRow = (GenericRowData) result;
        assertThat(resultRow.getArity()).isEqualTo(2);
        assertThat(resultRow.getField(0)).isEqualTo("converted_John");
        assertThat(resultRow.getField(1)).isEqualTo(300);

        // Test convertValue method for null
        assertThat(mapper.convertValueToFlinkType(null)).isNull();
    }

    @Test
    void testRowMapperWithNullFields() {
        CassandraFieldMapper[] fieldMappers = {mockElementMapper, mockValueMapper};
        String[] fieldNames = {"name", "age"};
        CollectionFieldMappers.RowMapper mapper =
                new CollectionFieldMappers.RowMapper(fieldMappers, fieldNames);

        // Test extracting UDT with null fields from Row
        when(mockRow.isNull("field")).thenReturn(false);
        when(mockRow.getObject("field")).thenReturn(mockUDTValue);
        when(mockUDTValue.isNull("name")).thenReturn(true);
        when(mockUDTValue.isNull("age")).thenReturn(false);
        when(mockUDTValue.getObject("age")).thenReturn(30);
        when(mockValueMapper.convertValueToFlinkType(30)).thenReturn(300);

        Object result = mapper.extractRowFieldValue(mockRow, "field");
        assertThat(result).isInstanceOf(GenericRowData.class);
        GenericRowData converted = (GenericRowData) result;
        assertThat(converted.getArity()).isEqualTo(2);
        assertThat(converted.isNullAt(0)).isTrue();
        assertThat(converted.getField(1)).isEqualTo(300);
    }

    @Test
    void testArrayMapperWithEmptyList() {
        CollectionFieldMappers.ArrayMapper mapper =
                new CollectionFieldMappers.ArrayMapper(mockElementMapper);

        // Test extracting empty list from Row
        when(mockRow.isNull("field")).thenReturn(false);
        List<Object> emptyList = new ArrayList<>();
        when(mockRow.getObject("field")).thenReturn(emptyList);

        Object result = mapper.extractRowFieldValue(mockRow, "field");
        assertThat(result).isInstanceOf(GenericArrayData.class);
        GenericArrayData converted = (GenericArrayData) result;
        assertThat(converted.size()).isEqualTo(0);
    }

    @Test
    void testMapMapperWithEmptyMap() {
        CollectionFieldMappers.MapMapper mapper =
                new CollectionFieldMappers.MapMapper(mockKeyMapper, mockValueMapper);

        // Test extracting empty map from Row
        when(mockRow.isNull("field")).thenReturn(false);
        Map<Object, Object> emptyMap = new HashMap<>();
        when(mockRow.getObject("field")).thenReturn(emptyMap);

        Object result = mapper.extractRowFieldValue(mockRow, "field");
        assertThat(result).isInstanceOf(GenericMapData.class);
        GenericMapData converted = (GenericMapData) result;
        assertThat(converted.size()).isEqualTo(0);
    }

    @Test
    void testSetMapperWithEmptySet() {
        CollectionFieldMappers.SetMapper mapper =
                new CollectionFieldMappers.SetMapper(mockElementMapper);

        // Test extracting empty set from Row
        when(mockRow.isNull("field")).thenReturn(false);
        Set<Object> emptySet = new HashSet<>();
        when(mockRow.getObject("field")).thenReturn(emptySet);

        Object result = mapper.extractRowFieldValue(mockRow, "field");
        assertThat(result).isInstanceOf(GenericMapData.class);
        GenericMapData converted = (GenericMapData) result;
        assertThat(converted.size()).isEqualTo(0);
    }

    @Test
    void testSetMapperCountTypeIsInteger() {
        CollectionFieldMappers.SetMapper mapper =
                new CollectionFieldMappers.SetMapper(mockElementMapper);

        // Test extracting integer set from Row to simulate MULTISET<INT>
        when(mockRow.isNull("field")).thenReturn(false);
        Set<Object> testSet = new HashSet<>(Arrays.asList(10, 20, 30));
        when(mockRow.getObject("field")).thenReturn(testSet);
        when(mockElementMapper.convertValueToFlinkType(10)).thenReturn(10);
        when(mockElementMapper.convertValueToFlinkType(20)).thenReturn(20);
        when(mockElementMapper.convertValueToFlinkType(30)).thenReturn(30);

        Object result = mapper.extractRowFieldValue(mockRow, "field");
        assertThat(result).isInstanceOf(GenericMapData.class);
        GenericMapData converted = (GenericMapData) result;
        assertThat(converted.size()).isEqualTo(3);

        // Verify that the count values are specifically Integer objects, not Long
        // This test ensures the fix for ClassCastException: Long cannot be cast to Integer
        Object count10 = converted.get(10);
        Object count20 = converted.get(20);
        Object count30 = converted.get(30);

        assertThat(count10).isInstanceOf(Integer.class);
        assertThat(count20).isInstanceOf(Integer.class);
        assertThat(count30).isInstanceOf(Integer.class);

        assertThat(count10).isEqualTo(1);
        assertThat(count20).isEqualTo(1);
        assertThat(count30).isEqualTo(1);

        // Additional check: verify it's not a Long
        assertThat(count10).isNotInstanceOf(Long.class);
        assertThat(count20).isNotInstanceOf(Long.class);
        assertThat(count30).isNotInstanceOf(Long.class);
    }

    @Test
    void testArrayMapperWithUnexpectedInputType() {
        CollectionFieldMappers.ArrayMapper mapper =
                new CollectionFieldMappers.ArrayMapper(mockElementMapper);

        // Test with non-List input (String instead of List)
        assertThatThrownBy(() -> mapper.convertValueToFlinkType("not_a_list"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Expected List, got:");

        // Test with non-List input (Integer instead of List)
        assertThatThrownBy(() -> mapper.convertValueToFlinkType(42))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Expected List, got:");

        // Test with Map instead of List
        Map<String, String> wrongType = new HashMap<>();
        wrongType.put("key", "value");
        assertThatThrownBy(() -> mapper.convertValueToFlinkType(wrongType))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Expected List, got:");
    }

    @Test
    void testMapMapperWithUnexpectedInputType() {
        CollectionFieldMappers.MapMapper mapper =
                new CollectionFieldMappers.MapMapper(mockKeyMapper, mockValueMapper);

        // Test with non-Map input (String instead of Map)
        assertThatThrownBy(() -> mapper.convertValueToFlinkType("not_a_map"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Expected Map, got:");

        // Test with non-Map input (List instead of Map)
        List<String> wrongType = Arrays.asList("a", "b", "c");
        assertThatThrownBy(() -> mapper.convertValueToFlinkType(wrongType))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Expected Map, got:");

        // Test with Set instead of Map
        Set<String> setType = new HashSet<>(Arrays.asList("x", "y"));
        assertThatThrownBy(() -> mapper.convertValueToFlinkType(setType))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Expected Map, got:");
    }

    @Test
    void testSetMapperWithUnexpectedInputType() {
        CollectionFieldMappers.SetMapper mapper =
                new CollectionFieldMappers.SetMapper(mockElementMapper);

        // Test with non-Set input (String instead of Set)
        assertThatThrownBy(() -> mapper.convertValueToFlinkType("not_a_set"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Expected Set, got:");

        // Test with non-Set input (Integer instead of Set)
        assertThatThrownBy(() -> mapper.convertValueToFlinkType(12345))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Expected Set, got:");

        // Test with List instead of Set
        List<String> listType = Arrays.asList("item1", "item2");
        assertThatThrownBy(() -> mapper.convertValueToFlinkType(listType))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Expected Set, got:");

        // Test with Map instead of Set
        Map<String, Integer> mapType = new HashMap<>();
        mapType.put("key", 100);
        assertThatThrownBy(() -> mapper.convertValueToFlinkType(mapType))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Expected Set, got:");
    }

    @Test
    void testRowMapperWithUnexpectedInputType() {
        CassandraFieldMapper[] fieldMappers = {mockElementMapper, mockValueMapper};
        String[] fieldNames = {"name", "age"};
        CollectionFieldMappers.RowMapper mapper =
                new CollectionFieldMappers.RowMapper(fieldMappers, fieldNames);

        // Test with non-UDTValue/TupleValue input (String instead of structured type)
        assertThatThrownBy(() -> mapper.convertValueToFlinkType("not_a_row"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Expected UDTValue or TupleValue, got:");

        // Test with List instead of structured type
        List<Object> listType = Arrays.asList("value1", "value2");
        assertThatThrownBy(() -> mapper.convertValueToFlinkType(listType))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Expected UDTValue or TupleValue, got:");

        // Test with Map instead of structured type
        Map<String, Object> mapType = new HashMap<>();
        mapType.put("name", "John");
        mapType.put("age", 30);
        assertThatThrownBy(() -> mapper.convertValueToFlinkType(mapType))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Expected UDTValue or TupleValue, got:");
    }
}
