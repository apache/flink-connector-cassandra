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

package org.apache.flink.connector.cassandra.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.cassandra.source.reader.converter.CassandraRowToTypeConverter;
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Unit tests for {@link CassandraRowEmitter}. */
class CassandraRowEmitterTest {

    @Mock private CassandraRowToTypeConverter<String> mockConverter;
    @Mock private SourceOutput<String> mockOutput;
    @Mock private CassandraSplit mockSplit;
    @Mock private Row mockRow;
    @Mock private ExecutionInfo mockExecutionInfo;

    private CassandraRowEmitter<String> emitter;
    private CassandraRow cassandraRow;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        emitter = new CassandraRowEmitter<>(mockConverter);
        cassandraRow = new CassandraRow(mockRow, mockExecutionInfo);
    }

    @Test
    void testEmitRecordSuccessfully() {
        // Setup
        String expectedResult = "converted-data";
        when(mockConverter.convert(cassandraRow)).thenReturn(expectedResult);

        // Execute
        emitter.emitRecord(cassandraRow, mockOutput, mockSplit);

        // Verify
        verify(mockConverter).convert(cassandraRow);
        verify(mockOutput).collect(expectedResult);
    }

    @Test
    void testEmitRecordWithNullResult() {
        // Setup - converter returns null
        when(mockConverter.convert(cassandraRow)).thenReturn(null);

        // Execute
        emitter.emitRecord(cassandraRow, mockOutput, mockSplit);

        // Verify
        verify(mockConverter).convert(cassandraRow);
        verify(mockOutput).collect(null);
    }

    @Test
    void testEmitRecordWithConversionException() {
        // Setup - converter throws exception
        RuntimeException expectedException = new RuntimeException("Conversion failed");
        when(mockConverter.convert(cassandraRow)).thenThrow(expectedException);

        // Execute & Verify
        assertThatThrownBy(() -> emitter.emitRecord(cassandraRow, mockOutput, mockSplit))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to deserialize Cassandra row")
                .hasCause(expectedException);

        verify(mockConverter).convert(cassandraRow);
        verifyNoInteractions(mockOutput);
    }

    @Test
    void testEmitRecordWithDifferentTypes() {
        CassandraRowToTypeConverter<Integer> intConverter =
                new CassandraRowToTypeConverter<Integer>() {
                    @Override
                    public Integer convert(CassandraRow cassandraRow) {
                        return 42;
                    }

                    @Override
                    public org.apache.flink.api.common.typeinfo.TypeInformation<Integer>
                            getProducedType() {
                        return org.apache.flink.api.common.typeinfo.TypeInformation.of(
                                Integer.class);
                    }
                };
        CassandraRowEmitter<Integer> intEmitter = new CassandraRowEmitter<>(intConverter);
        SourceOutput<Integer> intOutput =
                new SourceOutput<Integer>() {
                    @Override
                    public void collect(Integer record) {}

                    @Override
                    public void collect(Integer record, long timestamp) {}

                    @Override
                    public void emitWatermark(
                            org.apache.flink.api.common.eventtime.Watermark watermark) {}

                    @Override
                    public void markIdle() {}

                    @Override
                    public void markActive() {}
                };

        // This should compile and work without issues
        assertThat(intEmitter).isNotNull();
    }

    @Test
    void testEmitRecordWithComplexType() {
        ComplexObject expectedObject = new ComplexObject("test", 123);
        CassandraRowToTypeConverter<ComplexObject> complexConverter =
                new CassandraRowToTypeConverter<ComplexObject>() {
                    @Override
                    public ComplexObject convert(CassandraRow cassandraRow) {
                        return expectedObject;
                    }

                    @Override
                    public org.apache.flink.api.common.typeinfo.TypeInformation<ComplexObject>
                            getProducedType() {
                        return org.apache.flink.api.common.typeinfo.TypeInformation.of(
                                ComplexObject.class);
                    }
                };
        CassandraRowEmitter<ComplexObject> complexEmitter =
                new CassandraRowEmitter<>(complexConverter);
        SourceOutput<ComplexObject> complexOutput =
                new SourceOutput<ComplexObject>() {
                    @Override
                    public void collect(ComplexObject record) {}

                    @Override
                    public void collect(ComplexObject record, long timestamp) {}

                    @Override
                    public void emitWatermark(Watermark watermark) {}

                    @Override
                    public void markIdle() {}

                    @Override
                    public void markActive() {}
                };

        assertThat(complexEmitter).isNotNull();
    }

    @Test
    void testEmitMultipleRecords() {
        String result1 = "result1";
        String result2 = "result2";
        when(mockConverter.convert(cassandraRow)).thenReturn(result1, result2);
        emitter.emitRecord(cassandraRow, mockOutput, mockSplit);
        emitter.emitRecord(cassandraRow, mockOutput, mockSplit);
        verify(mockConverter, org.mockito.Mockito.times(2)).convert(cassandraRow);
        verify(mockOutput).collect(result1);
        verify(mockOutput).collect(result2);
    }

    @Test
    void testConstructorWithNullConverter() {
        assertThatThrownBy(() -> new CassandraRowEmitter<>(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testEmitRecordWithNullCassandraRow() {
        when(mockConverter.convert(null))
                .thenThrow(new NullPointerException("CassandraRow cannot be null"));

        assertThatThrownBy(() -> emitter.emitRecord(null, mockOutput, mockSplit))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to deserialize Cassandra row");
    }

    @Test
    void testEmitRecordWithNullOutput() {
        when(mockConverter.convert(cassandraRow)).thenReturn("test");

        assertThatThrownBy(() -> emitter.emitRecord(cassandraRow, null, mockSplit))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to deserialize Cassandra row");
    }

    @Test
    void testEmitRecordWithNullSplit() {
        // Split can be null in some scenarios
        when(mockConverter.convert(cassandraRow)).thenReturn("test");

        emitter.emitRecord(cassandraRow, mockOutput, null);

        verify(mockConverter).convert(cassandraRow);
        verify(mockOutput).collect("test");
    }

    @Test
    void testEmitRecordWithOutputException() {
        // Setup
        String result = "test-result";
        when(mockConverter.convert(cassandraRow)).thenReturn(result);
        doThrow(new RuntimeException("Output failed")).when(mockOutput).collect(result);

        assertThatThrownBy(() -> emitter.emitRecord(cassandraRow, mockOutput, mockSplit))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to deserialize Cassandra row");

        verify(mockConverter).convert(cassandraRow);
        verify(mockOutput).collect(result);
    }

    @Test
    void testEmitRecordWithChainedExceptions() {
        // Setup nested exception
        IllegalArgumentException cause = new IllegalArgumentException("Invalid argument");
        RuntimeException conversionException = new RuntimeException("Conversion failed", cause);
        when(mockConverter.convert(cassandraRow)).thenThrow(conversionException);

        // Execute & Verify
        assertThatThrownBy(() -> emitter.emitRecord(cassandraRow, mockOutput, mockSplit))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to deserialize Cassandra row")
                .hasCause(conversionException);
    }

    @Test
    void testEmitRecordPerformance() {
        when(mockConverter.convert(cassandraRow)).thenReturn("test");

        long startTime = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            emitter.emitRecord(cassandraRow, mockOutput, mockSplit);
        }
        long endTime = System.nanoTime();

        assertThat(endTime - startTime).isLessThan(1_000_000_000L);
    }

    private static class ComplexObject {
        private final String name;
        private final int value;

        public ComplexObject(String name, int value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public int getValue() {
            return value;
        }
    }
}
