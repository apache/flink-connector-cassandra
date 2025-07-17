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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.datastax.driver.core.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link CassandraRowToRowDataConverter}. */
class CassandraRowToRowDataConverterTest {

    @Mock private Row mockRow;

    private CassandraRowToRowDataConverter converter;
    private RowType rowType;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        rowType =
                RowType.of(
                        new VarCharType(255), // id column
                        new IntType(), // age column
                        new VarCharType(255) // name column
                        );
        converter = new CassandraRowToRowDataConverter(rowType);
    }

    @Test
    void testGetProducedType() {
        TypeInformation<RowData> result = converter.getProducedType();
        assertThat(result).isNotNull();
        assertThat(result.getTypeClass()).isEqualTo(RowData.class);
    }

    @Test
    void testConverterCreation() {
        RowType rowType = RowType.of(new IntType(), new VarCharType(255), new VarCharType(100));

        CassandraRowToRowDataConverter converter = new CassandraRowToRowDataConverter(rowType);

        assertThat(converter).isNotNull();
        assertThat(converter.getProducedType()).isNotNull();
    }

    @Test
    void testConstructorWithNullRowType() {
        assertThatThrownBy(() -> new CassandraRowToRowDataConverter(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testSerializability() {
        // Test that the converter is serializable
        assertThat(converter).isInstanceOf(java.io.Serializable.class);
    }

    @Test
    void testMultipleRowTypes() {
        // Test with different row types
        RowType simpleRowType = RowType.of(new IntType());
        CassandraRowToRowDataConverter simpleConverter =
                new CassandraRowToRowDataConverter(simpleRowType);

        assertThat(simpleConverter).isNotNull();
        assertThat(simpleConverter.getProducedType().getTypeClass()).isEqualTo(RowData.class);

        RowType complexRowType =
                RowType.of(
                        new VarCharType(100), new IntType(), new VarCharType(200), new IntType());
        CassandraRowToRowDataConverter complexConverter =
                new CassandraRowToRowDataConverter(complexRowType);

        assertThat(complexConverter).isNotNull();
        assertThat(complexConverter.getProducedType().getTypeClass()).isEqualTo(RowData.class);
    }
}
