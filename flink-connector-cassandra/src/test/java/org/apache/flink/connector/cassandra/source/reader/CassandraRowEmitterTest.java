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
import static org.mockito.Mockito.times;
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
    void testEmitMultipleRecords() {
        // Setup
        String result1 = "result1";
        String result2 = "result2";
        when(mockConverter.convert(cassandraRow)).thenReturn(result1, result2);

        // Execute
        emitter.emitRecord(cassandraRow, mockOutput, mockSplit);
        emitter.emitRecord(cassandraRow, mockOutput, mockSplit);

        // Verify
        verify(mockConverter, times(2)).convert(cassandraRow);
        verify(mockOutput).collect(result1);
        verify(mockOutput).collect(result2);
    }

    @Test
    void testConstructorWithNullConverter() {
        // Verify that null converter is not allowed
        assertThatThrownBy(() -> new CassandraRowEmitter<>(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Converter cannot be null");
    }

    @Test
    void testEmitRecordWithNullCassandraRow() {
        // Setup - converter throws NPE for null input
        when(mockConverter.convert(null))
                .thenThrow(new NullPointerException("CassandraRow cannot be null"));

        // Execute & Verify - null CassandraRow should result in wrapped exception
        assertThatThrownBy(() -> emitter.emitRecord(null, mockOutput, mockSplit))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to deserialize Cassandra row")
                .hasCauseInstanceOf(NullPointerException.class)
                .hasRootCauseMessage("CassandraRow cannot be null");

        // Verify converter was called with null
        verify(mockConverter).convert(null);
        verifyNoInteractions(mockOutput);
    }

    @Test
    void testEmitRecordWithNullOutput() {
        // Setup - converter returns valid result
        String result = "test";
        when(mockConverter.convert(cassandraRow)).thenReturn(result);

        // Execute & Verify - null output should cause NPE wrapped in RuntimeException
        assertThatThrownBy(() -> emitter.emitRecord(cassandraRow, null, mockSplit))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to deserialize Cassandra row")
                .hasCauseInstanceOf(NullPointerException.class);

        // Verify converter was called before the NPE occurred
        verify(mockConverter).convert(cassandraRow);
    }

    @Test
    void testEmitRecordWithOutputException() {
        // Setup - converter succeeds but output.collect() fails
        String result = "test-result";
        when(mockConverter.convert(cassandraRow)).thenReturn(result);
        RuntimeException outputException = new RuntimeException("Output failed");
        doThrow(outputException).when(mockOutput).collect(result);
        assertThatThrownBy(() -> emitter.emitRecord(cassandraRow, mockOutput, mockSplit))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to deserialize Cassandra row")
                .hasCause(outputException);
        verify(mockConverter).convert(cassandraRow);
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
        // Setup
        when(mockConverter.convert(cassandraRow)).thenReturn("test");

        // Execute multiple emissions and verify performance
        long startTime = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            emitter.emitRecord(cassandraRow, mockOutput, mockSplit);
        }
        long endTime = System.nanoTime();

        // Verify performance - should complete in reasonable time (5000ms for 1000 records)
        // Using a lenient threshold to avoid flaky failures in CI environments
        long durationInMillis = (endTime - startTime) / 1_000_000;
        assertThat(durationInMillis).isLessThan(5000L);

        // Verify the operations were called
        verify(mockConverter, times(1000)).convert(cassandraRow);
        verify(mockOutput, times(1000)).collect("test");
    }
}
