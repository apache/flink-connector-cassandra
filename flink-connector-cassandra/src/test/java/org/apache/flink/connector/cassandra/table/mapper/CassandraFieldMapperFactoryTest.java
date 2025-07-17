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

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
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
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

/** Unit tests for {@link CassandraFieldMapperFactory}. */
class CassandraFieldMapperFactoryTest {

    @Test
    void testPrimitiveTypeMappers() {
        CassandraFieldMapper booleanMapper =
                CassandraFieldMapperFactory.createFieldMapper(new BooleanType());
        assertThat(booleanMapper).isInstanceOf(PrimitiveFieldMappers.BooleanMapper.class);

        CassandraFieldMapper byteMapper =
                CassandraFieldMapperFactory.createFieldMapper(new TinyIntType());
        assertThat(byteMapper).isInstanceOf(PrimitiveFieldMappers.ByteMapper.class);

        CassandraFieldMapper shortMapper =
                CassandraFieldMapperFactory.createFieldMapper(new SmallIntType());
        assertThat(shortMapper).isInstanceOf(PrimitiveFieldMappers.ShortMapper.class);

        CassandraFieldMapper intMapper =
                CassandraFieldMapperFactory.createFieldMapper(new IntType());
        assertThat(intMapper).isInstanceOf(PrimitiveFieldMappers.IntegerMapper.class);

        CassandraFieldMapper longMapper =
                CassandraFieldMapperFactory.createFieldMapper(new BigIntType());
        assertThat(longMapper).isInstanceOf(PrimitiveFieldMappers.LongMapper.class);

        CassandraFieldMapper floatMapper =
                CassandraFieldMapperFactory.createFieldMapper(new FloatType());
        assertThat(floatMapper).isInstanceOf(PrimitiveFieldMappers.FloatMapper.class);

        CassandraFieldMapper doubleMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DoubleType());
        assertThat(doubleMapper).isInstanceOf(PrimitiveFieldMappers.DoubleMapper.class);

        CassandraFieldMapper stringMapper =
                CassandraFieldMapperFactory.createFieldMapper(VarCharType.STRING_TYPE);
        assertThat(stringMapper).isInstanceOf(PrimitiveFieldMappers.StringMapper.class);

        CassandraFieldMapper charMapper =
                CassandraFieldMapperFactory.createFieldMapper(new CharType(10));
        assertThat(charMapper).isInstanceOf(PrimitiveFieldMappers.StringMapper.class);

        CassandraFieldMapper dateMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DateType());
        assertThat(dateMapper).isInstanceOf(PrimitiveFieldMappers.DateMapper.class);

        CassandraFieldMapper timeMapper =
                CassandraFieldMapperFactory.createFieldMapper(new TimeType());
        assertThat(timeMapper).isInstanceOf(PrimitiveFieldMappers.TimeMapper.class);

        CassandraFieldMapper timestampMapper =
                CassandraFieldMapperFactory.createFieldMapper(new TimestampType());
        assertThat(timestampMapper).isInstanceOf(PrimitiveFieldMappers.TimestampMapper.class);

        CassandraFieldMapper binaryMapper =
                CassandraFieldMapperFactory.createFieldMapper(new BinaryType(10));
        assertThat(binaryMapper).isInstanceOf(PrimitiveFieldMappers.BinaryMapper.class);

        CassandraFieldMapper varbinaryMapper =
                CassandraFieldMapperFactory.createFieldMapper(new VarBinaryType(100));
        assertThat(varbinaryMapper).isInstanceOf(PrimitiveFieldMappers.BinaryMapper.class);
    }

    @Test
    void testDecimalMappers() {
        // Test that all decimal types now use DynamicDecimalMapper
        // This mapper determines the actual Cassandra type (varint vs decimal) at runtime

        // Test regular decimal precision
        CassandraFieldMapper decimalMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DecimalType(10, 2));
        assertThat(decimalMapper).isInstanceOf(PrimitiveFieldMappers.DynamicDecimalMapper.class);

        // Test high precision decimal
        CassandraFieldMapper highPrecisionMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DecimalType(25, 5));
        assertThat(highPrecisionMapper)
                .isInstanceOf(PrimitiveFieldMappers.DynamicDecimalMapper.class);

        // Test edge case: exactly 18 precision
        CassandraFieldMapper edgeDecimalMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DecimalType(18, 2));
        assertThat(edgeDecimalMapper)
                .isInstanceOf(PrimitiveFieldMappers.DynamicDecimalMapper.class);

        // Test edge case: 19 precision
        CassandraFieldMapper edgeVarintMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DecimalType(19, 2));
        assertThat(edgeVarintMapper).isInstanceOf(PrimitiveFieldMappers.DynamicDecimalMapper.class);
    }

    @Test
    void testArrayMapper() {
        ArrayType stringArrayType = new ArrayType(VarCharType.STRING_TYPE);
        CassandraFieldMapper arrayMapper =
                CassandraFieldMapperFactory.createFieldMapper(stringArrayType);
        assertThat(arrayMapper).isInstanceOf(CollectionFieldMappers.ArrayMapper.class);

        ArrayType nestedArrayType = new ArrayType(new ArrayType(new IntType()));
        CassandraFieldMapper nestedArrayMapper =
                CassandraFieldMapperFactory.createFieldMapper(nestedArrayType);
        assertThat(nestedArrayMapper).isInstanceOf(CollectionFieldMappers.ArrayMapper.class);
    }

    @Test
    void testMapMapper() {
        MapType mapType = new MapType(VarCharType.STRING_TYPE, new IntType());
        CassandraFieldMapper mapMapper = CassandraFieldMapperFactory.createFieldMapper(mapType);
        assertThat(mapMapper).isInstanceOf(CollectionFieldMappers.MapMapper.class);

        MapType nestedMapType =
                new MapType(
                        VarCharType.STRING_TYPE,
                        new MapType(VarCharType.STRING_TYPE, new IntType()));
        CassandraFieldMapper nestedMapMapper =
                CassandraFieldMapperFactory.createFieldMapper(nestedMapType);
        assertThat(nestedMapMapper).isInstanceOf(CollectionFieldMappers.MapMapper.class);
    }

    @Test
    void testMultisetMapper() {
        MultisetType multisetType = new MultisetType(VarCharType.STRING_TYPE);
        CassandraFieldMapper setMapper =
                CassandraFieldMapperFactory.createFieldMapper(multisetType);
        assertThat(setMapper).isInstanceOf(CollectionFieldMappers.SetMapper.class);

        MultisetType nestedMultisetType = new MultisetType(new ArrayType(new IntType()));
        CassandraFieldMapper nestedSetMapper =
                CassandraFieldMapperFactory.createFieldMapper(nestedMultisetType);
        assertThat(nestedSetMapper).isInstanceOf(CollectionFieldMappers.SetMapper.class);
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
        assertThat(rowMapper).isInstanceOf(CollectionFieldMappers.RowMapper.class);

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
        assertThat(nestedRowMapper).isInstanceOf(CollectionFieldMappers.RowMapper.class);
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
        assertThat(complexMapper).isInstanceOf(CollectionFieldMappers.ArrayMapper.class);
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
        assertThat(complexRowMapper).isInstanceOf(CollectionFieldMappers.RowMapper.class);
    }

    @Test
    void testRecursiveMapperCreation() {
        RowType deepRowType =
                RowType.of(
                        new LogicalType[] {new IntType(), new MultisetType(new DoubleType())},
                        new String[] {"id", "values"});
        MapType deepMapType = new MapType(VarCharType.STRING_TYPE, deepRowType);
        ArrayType deepArrayType = new ArrayType(new ArrayType(deepMapType));

        CassandraFieldMapper deepMapper =
                CassandraFieldMapperFactory.createFieldMapper(deepArrayType);
        assertThat(deepMapper).isInstanceOf(CollectionFieldMappers.ArrayMapper.class);
        assertThat(deepMapper).isNotNull();
    }

    @Test
    void testUnsupportedTypeThrowsException() {
        ZonedTimestampType zonedTimestampType = new ZonedTimestampType();

        UnsupportedOperationException exception1 =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> CassandraFieldMapperFactory.createFieldMapper(zonedTimestampType));

        assertThat(exception1.getMessage())
                .contains("TIMESTAMP_WITH_TIME_ZONE is not supported")
                .contains("timezone-naive")
                .contains("UTC");

        LocalZonedTimestampType localZonedType = new LocalZonedTimestampType();

        UnsupportedOperationException exception2 =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> CassandraFieldMapperFactory.createFieldMapper(localZonedType));

        assertThat(exception2.getMessage())
                .contains("TIMESTAMP_WITH_LOCAL_TIME_ZONE is not supported")
                .contains("timezone-naive");

        // Test interval types
        YearMonthIntervalType yearMonthInterval =
                new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH);

        UnsupportedOperationException exception3 =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> CassandraFieldMapperFactory.createFieldMapper(yearMonthInterval));

        assertThat(exception3.getMessage())
                .contains("INTERVAL_YEAR_MONTH is not supported")
                .contains("native interval types")
                .contains("BIGINT");

        // Test day-time interval
        DayTimeIntervalType dayTimeInterval =
                new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND);

        UnsupportedOperationException exception4 =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> CassandraFieldMapperFactory.createFieldMapper(dayTimeInterval));

        assertThat(exception4.getMessage())
                .contains("INTERVAL_DAY_TIME is not supported")
                .contains("native interval types");
    }
}
