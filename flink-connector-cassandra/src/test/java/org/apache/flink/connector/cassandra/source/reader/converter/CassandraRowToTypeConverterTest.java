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

package org.apache.flink.connector.cassandra.source.reader.converter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.cassandra.source.reader.CassandraRow;

import com.datastax.driver.core.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/** Unit tests for {@link CassandraRowToTypeConverter} interface. */
class CassandraRowToTypeConverterTest {

    @Mock private Row mockRow;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testStringConverter() {
        // Test simple string conversion
        CassandraRowToTypeConverter<String> converter =
                new CassandraRowToTypeConverter<String>() {
                    @Override
                    public String convert(CassandraRow cassandraRow) {
                        return "test-string";
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                };

        String result = converter.convert(new CassandraRow(mockRow, null));
        assertThat(result).isEqualTo("test-string");
        assertThat(converter.getProducedType()).isEqualTo(Types.STRING);
    }

    @Test
    void testIntegerConverter() {
        // Test integer conversion
        when(mockRow.getInt("value")).thenReturn(42);

        CassandraRowToTypeConverter<Integer> converter =
                new CassandraRowToTypeConverter<Integer>() {
                    @Override
                    public Integer convert(CassandraRow cassandraRow) {
                        return cassandraRow.getRow().getInt("value");
                    }

                    @Override
                    public TypeInformation<Integer> getProducedType() {
                        return Types.INT;
                    }
                };

        Integer result = converter.convert(new CassandraRow(mockRow, null));
        assertThat(result).isEqualTo(42);
        assertThat(converter.getProducedType()).isEqualTo(Types.INT);
    }

    @Test
    void testNullConverter() {
        // Test converter that returns null
        CassandraRowToTypeConverter<String> converter =
                new CassandraRowToTypeConverter<String>() {
                    @Override
                    public String convert(CassandraRow cassandraRow) {
                        return null;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                };

        String result = converter.convert(new CassandraRow(mockRow, null));
        assertThat(result).isNull();
    }

    @Test
    void testComplexObjectConverter() {
        // Test conversion to complex object
        when(mockRow.getString("name")).thenReturn("Test Name");
        when(mockRow.getInt("id")).thenReturn(123);

        CassandraRowToTypeConverter<ComplexObject> converter =
                new CassandraRowToTypeConverter<ComplexObject>() {
                    @Override
                    public ComplexObject convert(CassandraRow cassandraRow) {
                        Row row = cassandraRow.getRow();
                        return new ComplexObject(row.getString("name"), row.getInt("id"));
                    }

                    @Override
                    public TypeInformation<ComplexObject> getProducedType() {
                        return TypeInformation.of(ComplexObject.class);
                    }
                };

        ComplexObject result = converter.convert(new CassandraRow(mockRow, null));
        assertThat(result).isNotNull();
        assertThat(result.getName()).isEqualTo("Test Name");
        assertThat(result.getId()).isEqualTo(123);
        assertThat(converter.getProducedType().getTypeClass()).isEqualTo(ComplexObject.class);
    }

    @Test
    void testConverterWithException() {
        // Test converter that throws exception
        CassandraRowToTypeConverter<String> converter =
                new CassandraRowToTypeConverter<String>() {
                    @Override
                    public String convert(CassandraRow cassandraRow) {
                        throw new RuntimeException("Conversion failed");
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                };

        assertThatThrownBy(() -> converter.convert(new CassandraRow(mockRow, null)))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Conversion failed");
    }

    @Test
    void testSerializableConverter() throws Exception {
        // Test that converter implementations are serializable
        TestSerializableConverter converter = new TestSerializableConverter();

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(converter);
        oos.close();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        TestSerializableConverter deserializedConverter =
                (TestSerializableConverter) ois.readObject();
        ois.close();

        // Test deserialized converter
        String result = deserializedConverter.convert(new CassandraRow(mockRow, null));
        assertThat(result).isEqualTo("serializable-test");
        assertThat(deserializedConverter.getProducedType()).isEqualTo(Types.STRING);
    }

    /** Test converter that is properly serializable. */
    private static class TestSerializableConverter implements CassandraRowToTypeConverter<String> {
        @Override
        public String convert(CassandraRow cassandraRow) {
            return "serializable-test";
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

    @Test
    void testStatefulConverter() {
        // Test converter with internal state
        CassandraRowToTypeConverter<Integer> converter =
                new CassandraRowToTypeConverter<Integer>() {
                    private final AtomicInteger counter = new AtomicInteger(0);

                    @Override
                    public Integer convert(CassandraRow cassandraRow) {
                        return counter.incrementAndGet();
                    }

                    @Override
                    public TypeInformation<Integer> getProducedType() {
                        return Types.INT;
                    }
                };

        // Test that state is maintained across calls
        Integer result1 = converter.convert(new CassandraRow(mockRow, null));
        Integer result2 = converter.convert(new CassandraRow(mockRow, null));
        Integer result3 = converter.convert(new CassandraRow(mockRow, null));

        assertThat(result1).isEqualTo(1);
        assertThat(result2).isEqualTo(2);
        assertThat(result3).isEqualTo(3);
    }

    @Test
    void testGenericTypeConverter() {
        // Test converter with generic types
        CassandraRowToTypeConverter<java.util.List<String>> converter =
                new CassandraRowToTypeConverter<java.util.List<String>>() {
                    @Override
                    public java.util.List<String> convert(CassandraRow cassandraRow) {
                        return java.util.Arrays.asList("item1", "item2", "item3");
                    }

                    @Override
                    public TypeInformation<java.util.List<String>> getProducedType() {
                        return TypeInformation.of(
                                new org.apache.flink.api.common.typeinfo.TypeHint<
                                        java.util.List<String>>() {});
                    }
                };

        java.util.List<String> result = converter.convert(new CassandraRow(mockRow, null));
        assertThat(result).hasSize(3);
        assertThat(result).containsExactly("item1", "item2", "item3");
    }

    @Test
    void testConverterChaining() {
        // Test that converters can be chained
        CassandraRowToTypeConverter<String> stringConverter =
                new CassandraRowToTypeConverter<String>() {
                    @Override
                    public String convert(CassandraRow cassandraRow) {
                        return "42";
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                };

        CassandraRowToTypeConverter<Integer> intConverter =
                new CassandraRowToTypeConverter<Integer>() {
                    @Override
                    public Integer convert(CassandraRow cassandraRow) {
                        String str = stringConverter.convert(cassandraRow);
                        return Integer.parseInt(str);
                    }

                    @Override
                    public TypeInformation<Integer> getProducedType() {
                        return Types.INT;
                    }
                };

        Integer result = intConverter.convert(new CassandraRow(mockRow, null));
        assertThat(result).isEqualTo(42);
    }

    @Test
    void testConverterWithRowAccess() {
        // Test direct row access
        when(mockRow.getString("field1")).thenReturn("value1");
        when(mockRow.getInt("field2")).thenReturn(100);
        when(mockRow.getBool("field3")).thenReturn(true);

        CassandraRowToTypeConverter<String> converter =
                new CassandraRowToTypeConverter<String>() {
                    @Override
                    public String convert(CassandraRow cassandraRow) {
                        Row row = cassandraRow.getRow();
                        return String.format(
                                "%s-%d-%b",
                                row.getString("field1"),
                                row.getInt("field2"),
                                row.getBool("field3"));
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                };

        String result = converter.convert(new CassandraRow(mockRow, null));
        assertThat(result).isEqualTo("value1-100-true");
    }

    /** Helper class for testing complex object conversion. */
    public static class ComplexObject {
        private final String name;
        private final int id;

        public ComplexObject(String name, int id) {
            this.name = name;
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public int getId() {
            return id;
        }
    }
}
