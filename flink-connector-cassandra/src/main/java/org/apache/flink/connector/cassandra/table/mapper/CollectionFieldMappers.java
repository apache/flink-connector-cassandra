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
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Collection field mappers for handling ARRAY and MAP types. */
@Internal
public final class CollectionFieldMappers {

    private CollectionFieldMappers() {}

    /** Array field mapper that delegates element mapping to provided element mapper. */
    public static final class ArrayMapper implements CassandraFieldMapper {
        private final CassandraFieldMapper fieldMapper;

        public ArrayMapper(CassandraFieldMapper fieldMapper) {
            this.fieldMapper = fieldMapper;
        }

        @Override
        public Object extractFromRow(Row row, String fieldName) {
            if (row.isNull(fieldName)) {
                return null;
            }
            Object rawValue = row.getObject(fieldName);
            return convertValue(rawValue);
        }

        @Override
        public Object convertValue(Object value) {
            if (value == null) {
                return null;
            }

            if (!(value instanceof List)) {
                throw new IllegalArgumentException("Expected List, got: " + value.getClass());
            }

            List<?> list = (List<?>) value;
            Object[] array = new Object[list.size()];
            for (int i = 0; i < list.size(); i++) {
                array[i] = fieldMapper.convertValue(list.get(i));
            }
            return new GenericArrayData(array);
        }
    }

    /** Map field mapper that delegates key and value mapping to provided field mappers. */
    public static final class MapMapper implements CassandraFieldMapper {
        private final CassandraFieldMapper keyConverter;
        private final CassandraFieldMapper valueConverter;

        public MapMapper(CassandraFieldMapper keyConverter, CassandraFieldMapper valueConverter) {
            this.keyConverter = keyConverter;
            this.valueConverter = valueConverter;
        }

        @Override
        public Object extractFromRow(Row row, String fieldName) {
            if (row.isNull(fieldName)) {
                return null;
            }
            Object rawValue = row.getObject(fieldName);
            return convertValue(rawValue);
        }

        @Override
        public Object convertValue(Object value) {
            if (value == null) {
                return null;
            }

            if (!(value instanceof Map)) {
                throw new IllegalArgumentException("Expected Map, got: " + value.getClass());
            }

            Map<?, ?> map = (Map<?, ?>) value;
            Map<Object, Object> resultMap = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object convertedKey = keyConverter.convertValue(entry.getKey());
                Object convertedValue = valueConverter.convertValue(entry.getValue());
                resultMap.put(convertedKey, convertedValue);
            }
            return new GenericMapData(resultMap);
        }
    }

    /** Set field mapper that handles Cassandra set types as multisets (maps element to count). */
    public static final class SetMapper implements CassandraFieldMapper {
        private final CassandraFieldMapper fieldMapper;

        public SetMapper(CassandraFieldMapper fieldMapper) {
            this.fieldMapper = fieldMapper;
        }

        @Override
        public Object extractFromRow(Row row, String fieldName) {
            if (row.isNull(fieldName)) {
                return null;
            }
            Object rawValue = row.getObject(fieldName);
            return convertValue(rawValue);
        }

        @Override
        public Object convertValue(Object value) {
            if (value == null) {
                return null;
            }

            if (!(value instanceof Set)) {
                throw new IllegalArgumentException("Expected Set, got: " + value.getClass());
            }

            Set<?> set = (Set<?>) value;
            Map<Object, Object> resultMap = new HashMap<>();
            for (Object element : set) {
                Object convertedElement = fieldMapper.convertValue(element);
                resultMap.put(convertedElement, 1); // Each element in a set has count 1
            }
            return new GenericMapData(resultMap);
        }
    }

    /** Row field mapper for nested row types with recursive UDT support. */
    public static final class RowMapper implements CassandraFieldMapper {
        private final CassandraFieldMapper[] fieldMappers;
        private final String[] fieldNames;
        private final int fieldCount;

        public RowMapper(CassandraFieldMapper[] fieldMappers, String[] fieldNames) {
            this.fieldMappers = fieldMappers;
            this.fieldNames = fieldNames;
            this.fieldCount = fieldMappers.length;
        }

        @Override
        public Object extractFromRow(Row row, String fieldName) {
            if (row.isNull(fieldName)) {
                return null;
            }
            // Use getObject to handle both UDTs and tuples
            Object rawValue = row.getObject(fieldName);
            return convertValue(rawValue);
        }

        @Override
        public Object convertValue(Object value) {
            if (value == null) {
                return null;
            }

            GenericRowData rowData = new GenericRowData(fieldCount);

            if (value instanceof UDTValue) {
                UDTValue udtValue = (UDTValue) value;
                for (int i = 0; i < fieldCount; i++) {
                    String nestedFieldName = fieldNames[i];
                    Object fieldValue = null;

                    if (!udtValue.isNull(nestedFieldName)) {
                        Object rawValue = udtValue.getObject(nestedFieldName);
                        fieldValue = fieldMappers[i].convertValue(rawValue);
                    }

                    rowData.setField(i, fieldValue);
                }
            } else if (value instanceof TupleValue) {
                TupleValue tupleValue = (TupleValue) value;
                for (int i = 0; i < fieldCount; i++) {
                    Object fieldValue = null;
                    if (!tupleValue.isNull(i)) {
                        Object rawValue = tupleValue.getObject(i);
                        fieldValue = fieldMappers[i].convertValue(rawValue);
                    }
                    rowData.setField(i, fieldValue);
                }
            } else {
                throw new IllegalArgumentException(
                        "Expected UDTValue or TupleValue, got: " + value.getClass());
            }

            return rowData;
        }
    }
}
