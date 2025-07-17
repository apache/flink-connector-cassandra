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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Duration;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for {@link PrimitiveFieldMappers}. */
class PrimitiveFieldMappersTest {

    @Mock private Row mockRow;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testBooleanMapper() {
        PrimitiveFieldMappers.BooleanMapper mapper = new PrimitiveFieldMappers.BooleanMapper();

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test true value
        when(mockRow.isNull("field")).thenReturn(false);
        when(mockRow.getBool("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isEqualTo(true);

        // Test false value
        when(mockRow.getBool("field")).thenReturn(false);
        assertThat(mapper.extractFromRow(mockRow, "field")).isEqualTo(false);
    }

    @Test
    void testByteMapper() {
        PrimitiveFieldMappers.ByteMapper mapper = new PrimitiveFieldMappers.ByteMapper();

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test byte value
        when(mockRow.isNull("field")).thenReturn(false);
        when(mockRow.getByte("field")).thenReturn((byte) 42);
        assertThat(mapper.extractFromRow(mockRow, "field")).isEqualTo((byte) 42);
    }

    @Test
    void testShortMapper() {
        PrimitiveFieldMappers.ShortMapper mapper = new PrimitiveFieldMappers.ShortMapper();

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test short value
        when(mockRow.isNull("field")).thenReturn(false);
        when(mockRow.getShort("field")).thenReturn((short) 1000);
        assertThat(mapper.extractFromRow(mockRow, "field")).isEqualTo((short) 1000);
    }

    @Test
    void testIntegerMapper() {
        PrimitiveFieldMappers.IntegerMapper mapper = new PrimitiveFieldMappers.IntegerMapper();

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test integer value
        when(mockRow.isNull("field")).thenReturn(false);
        when(mockRow.getInt("field")).thenReturn(12345);
        assertThat(mapper.extractFromRow(mockRow, "field")).isEqualTo(12345);

        // Test convertValue method with Long input (fixes ClassCastException)
        assertThat(mapper.convertValue(null)).isNull();
        assertThat(mapper.convertValue(42)).isEqualTo(42);
        assertThat(mapper.convertValue(42L)).isEqualTo(42); // Long to int conversion
        assertThat(mapper.convertValue((short) 100)).isEqualTo(100); // Short to int conversion
        assertThat(mapper.convertValue((byte) 5)).isEqualTo(5); // Byte to int conversion
    }

    @Test
    void testLongMapper() {
        PrimitiveFieldMappers.LongMapper mapper = new PrimitiveFieldMappers.LongMapper();

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test long value
        when(mockRow.isNull("field")).thenReturn(false);
        when(mockRow.getLong("field")).thenReturn(123456789L);
        assertThat(mapper.extractFromRow(mockRow, "field")).isEqualTo(123456789L);

        assertThat(mapper.convertValue(null)).isNull();
        assertThat(mapper.convertValue(42L)).isEqualTo(42L);
        assertThat(mapper.convertValue(42)).isEqualTo(42L); // Integer to long conversion
        assertThat(mapper.convertValue((short) 100)).isEqualTo(100L); // Short to long conversion
        assertThat(mapper.convertValue((byte) 5)).isEqualTo(5L); // Byte to long conversion
    }

    @Test
    void testFloatMapper() {
        PrimitiveFieldMappers.FloatMapper mapper = new PrimitiveFieldMappers.FloatMapper();

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test float value
        when(mockRow.isNull("field")).thenReturn(false);
        when(mockRow.getFloat("field")).thenReturn(3.14f);
        assertThat(mapper.extractFromRow(mockRow, "field")).isEqualTo(3.14f);
    }

    @Test
    void testDoubleMapper() {
        PrimitiveFieldMappers.DoubleMapper mapper = new PrimitiveFieldMappers.DoubleMapper();

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test double value
        when(mockRow.isNull("field")).thenReturn(false);
        when(mockRow.getDouble("field")).thenReturn(2.71828);
        assertThat(mapper.extractFromRow(mockRow, "field")).isEqualTo(2.71828);
    }

    @Test
    void testStringMapper() throws Exception {
        PrimitiveFieldMappers.StringMapper mapper = new PrimitiveFieldMappers.StringMapper();

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test string value (text type)
        when(mockRow.isNull("field")).thenReturn(false);
        ColumnDefinitions columnDefs = mock(ColumnDefinitions.class);
        DataType dataType = mock(DataType.class);
        DataType.Name typeName = mock(DataType.Name.class);
        when(mockRow.getColumnDefinitions()).thenReturn(columnDefs);
        when(columnDefs.getType("field")).thenReturn(dataType);
        when(dataType.getName()).thenReturn(typeName);
        when(typeName.toString()).thenReturn("text");
        when(mockRow.getString("field")).thenReturn("hello");
        Object result = mapper.extractFromRow(mockRow, "field");
        assertThat(result).isInstanceOf(StringData.class);
        assertThat(result.toString()).isEqualTo("hello");

        // Test inet value
        when(typeName.toString()).thenReturn("inet");
        InetAddress testAddress = InetAddress.getByName("192.168.1.1");
        when(mockRow.getInet("field")).thenReturn(testAddress);
        Object inetResult = mapper.extractFromRow(mockRow, "field");
        assertThat(inetResult).isInstanceOf(StringData.class);
        assertThat(inetResult.toString()).isEqualTo("192.168.1.1");

        // Test duration value
        when(typeName.toString()).thenReturn("duration");
        Duration testDuration = Duration.newInstance(1, 2, 3000000000L);
        when(mockRow.get("field", Duration.class)).thenReturn(testDuration);
        Object durationResult = mapper.extractFromRow(mockRow, "field");
        assertThat(durationResult).isInstanceOf(StringData.class);
        assertThat(durationResult.toString()).isEqualTo(testDuration.toString());

        // Test convertValue method
        assertThat(mapper.convertValue(null)).isNull();
        StringData converted = (StringData) mapper.convertValue("test");
        assertThat(converted.toString()).isEqualTo("test");
    }

    @Test
    void testDecimalMapper() {
        DecimalType decimalType = new DecimalType(10, 2);
        PrimitiveFieldMappers.DecimalMapper mapper =
                new PrimitiveFieldMappers.DecimalMapper(decimalType);

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test decimal value
        when(mockRow.isNull("field")).thenReturn(false);
        BigDecimal testDecimal = new BigDecimal("123.45");
        when(mockRow.getDecimal("field")).thenReturn(testDecimal);
        Object result = mapper.extractFromRow(mockRow, "field");
        assertThat(result).isInstanceOf(DecimalData.class);

        // Test convertValue method
        assertThat(mapper.convertValue(null)).isNull();
        DecimalData converted = (DecimalData) mapper.convertValue(testDecimal);
        assertThat(converted.toBigDecimal()).isEqualTo(testDecimal);
    }

    @Test
    void testDateMapper() {
        PrimitiveFieldMappers.DateMapper mapper = new PrimitiveFieldMappers.DateMapper();

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test date value
        when(mockRow.isNull("field")).thenReturn(false);
        LocalDate testDate = LocalDate.fromYearMonthDay(2023, 6, 15);
        when(mockRow.getDate("field")).thenReturn(testDate);
        Object result = mapper.extractFromRow(mockRow, "field");
        assertThat(result).isInstanceOf(Integer.class);

        // Test convertValue method
        assertThat(mapper.convertValue(null)).isNull();
        Integer converted = (Integer) mapper.convertValue(testDate);
        assertThat(converted).isEqualTo((int) java.time.LocalDate.of(2023, 6, 15).toEpochDay());
    }

    @Test
    void testTimeMapper() {
        PrimitiveFieldMappers.TimeMapper mapper = new PrimitiveFieldMappers.TimeMapper();

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test time value - Cassandra time is nanoseconds since midnight
        when(mockRow.isNull("field")).thenReturn(false);
        long testTimeNanos =
                14L * 3600 * 1_000_000_000
                        + 30L * 60 * 1_000_000_000
                        + 45L * 1_000_000_000; // 14:30:45
        when(mockRow.getTime("field")).thenReturn(testTimeNanos);
        Object result = mapper.extractFromRow(mockRow, "field");
        assertThat(result).isInstanceOf(Integer.class);

        // Expected: 14:30:45 = 14*3600*1000 + 30*60*1000 + 45*1000 = 52245000 milliseconds
        int expectedMillis = 14 * 3600 * 1000 + 30 * 60 * 1000 + 45 * 1000;
        assertThat(result).isEqualTo(expectedMillis);

        // Test convertValue method
        assertThat(mapper.convertValue(null)).isNull();
        assertThat(mapper.convertValue(1_000_000_000L))
                .isEqualTo(1000); // 1 second in nanos -> 1000 millis
        assertThat(mapper.convertValue(500_000_000L))
                .isEqualTo(500); // 0.5 second in nanos -> 500 millis
    }

    @Test
    void testTimestampMapper() {
        PrimitiveFieldMappers.TimestampMapper mapper = new PrimitiveFieldMappers.TimestampMapper();

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test timestamp value
        when(mockRow.isNull("field")).thenReturn(false);
        Date testDate = new Date(1234567890000L);
        when(mockRow.getTimestamp("field")).thenReturn(testDate);
        Object result = mapper.extractFromRow(mockRow, "field");
        assertThat(result).isInstanceOf(TimestampData.class);

        // Test convertValue method with Date
        assertThat(mapper.convertValue(null)).isNull();
        TimestampData converted = (TimestampData) mapper.convertValue(testDate);
        assertThat(converted.toInstant()).isEqualTo(testDate.toInstant());

        // Test convertValue method with Instant
        Instant instant = Instant.ofEpochMilli(1234567890000L);
        TimestampData convertedInstant = (TimestampData) mapper.convertValue(instant);
        assertThat(convertedInstant.toInstant()).isEqualTo(instant);
    }

    @Test
    void testBinaryMapper() {
        PrimitiveFieldMappers.BinaryMapper mapper = new PrimitiveFieldMappers.BinaryMapper();

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test binary value
        when(mockRow.isNull("field")).thenReturn(false);
        ByteBuffer testBuffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
        when(mockRow.getBytes("field")).thenReturn(testBuffer);
        Object result = mapper.extractFromRow(mockRow, "field");
        assertThat(result).isInstanceOf(byte[].class);

        // Test convertValue method
        assertThat(mapper.convertValue(null)).isNull();
        ByteBuffer testBuffer2 = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
        byte[] converted = (byte[]) mapper.convertValue(testBuffer2);
        assertThat(converted).isEqualTo(new byte[] {1, 2, 3, 4});

        // Test convertValue with byte array
        byte[] testArray = new byte[] {5, 6, 7, 8};
        byte[] convertedArray = (byte[]) mapper.convertValue(testArray);
        assertThat(convertedArray).isEqualTo(testArray);
    }

    @Test
    void testVarintMapper() {
        DecimalType decimalType = new DecimalType(38, 0);
        PrimitiveFieldMappers.VarintMapper mapper =
                new PrimitiveFieldMappers.VarintMapper(decimalType);

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test varint value
        when(mockRow.isNull("field")).thenReturn(false);
        BigInteger testVarint = new BigInteger("123456789012345678901234567890");
        when(mockRow.getVarint("field")).thenReturn(testVarint);
        Object result = mapper.extractFromRow(mockRow, "field");
        assertThat(result).isInstanceOf(DecimalData.class);

        // Test convertValue method
        assertThat(mapper.convertValue(null)).isNull();
        DecimalData converted = (DecimalData) mapper.convertValue(testVarint);
        assertThat(converted.toBigDecimal()).isEqualTo(new BigDecimal(testVarint));
    }
}
