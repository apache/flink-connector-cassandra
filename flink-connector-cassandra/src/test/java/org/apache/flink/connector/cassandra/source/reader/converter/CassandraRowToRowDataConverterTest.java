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
import org.apache.flink.connector.cassandra.source.reader.CassandraRow;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/** Unit tests for {@link CassandraRowToRowDataConverter}. */
class CassandraRowToRowDataConverterTest {

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
        assertThat(converter.getProducedType().getTypeClass()).isEqualTo(RowData.class);
    }

    @Test
    void testConstructorWithNullRowType() {
        assertThatThrownBy(() -> new CassandraRowToRowDataConverter(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testMultipleRowTypes() {
        RowType rowType1 = RowType.of(new IntType(), new VarCharType(255), new VarCharType(100));
        CassandraRowToRowDataConverter converter1 = new CassandraRowToRowDataConverter(rowType1);

        assertThat(converter1).isNotNull();
        assertThat(converter1.getProducedType()).isNotNull();
        assertThat(converter1.getProducedType().getTypeClass()).isEqualTo(RowData.class);

        RowType rowType2 =
                RowType.of(
                        new LogicalType[] {
                            new VarCharType(50), new IntType(), new IntType(), new VarCharType(200)
                        },
                        new String[] {"id", "age", "score", "description"});
        CassandraRowToRowDataConverter converter2 = new CassandraRowToRowDataConverter(rowType2);

        assertThat(converter2).isNotNull();
        assertThat(converter2.getProducedType()).isNotNull();
        assertThat(converter2.getProducedType().getTypeClass()).isEqualTo(RowData.class);

        RowType rowType3 = RowType.of(new IntType());
        CassandraRowToRowDataConverter converter3 = new CassandraRowToRowDataConverter(rowType3);

        assertThat(converter3).isNotNull();
        assertThat(converter3.getProducedType()).isNotNull();
        assertThat(converter3.getProducedType().getTypeClass()).isEqualTo(RowData.class);
    }

    @Test
    void testSerializability() throws Exception {
        // Create a converter with specific field types and names
        RowType originalRowType =
                RowType.of(
                        new LogicalType[] {
                            new IntType(), new VarCharType(255), new VarCharType(100)
                        },
                        new String[] {"age", "name", "city"});

        // Create the original converter
        CassandraRowToRowDataConverter originalConverter =
                new CassandraRowToRowDataConverter(originalRowType);

        // Serialize the converter
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(originalConverter);
        oos.close();

        // Deserialize the converter
        byte[] serializedData = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(serializedData);
        ObjectInputStream ois = new ObjectInputStream(bais);
        CassandraRowToRowDataConverter deserializedConverter =
                (CassandraRowToRowDataConverter) ois.readObject();
        ois.close();

        // Verify basic properties of the deserialized converter
        assertThat(deserializedConverter).isNotNull();
        assertThat(deserializedConverter.getProducedType()).isNotNull();
        assertThat(deserializedConverter.getProducedType().getTypeClass()).isEqualTo(RowData.class);

        // Set up mock Cassandra row data
        Row testRow = Mockito.mock(Row.class);
        CassandraRow testCassandraRow = Mockito.mock(CassandraRow.class);
        ColumnDefinitions columnDefinitions = Mockito.mock(ColumnDefinitions.class);
        when(testCassandraRow.getRow()).thenReturn(testRow);
        when(testRow.getColumnDefinitions()).thenReturn(columnDefinitions);

        // Mock column types to match the original RowType
        when(columnDefinitions.getType("age")).thenReturn(DataType.cint());
        when(columnDefinitions.getType("name")).thenReturn(DataType.varchar());
        when(columnDefinitions.getType("city")).thenReturn(DataType.varchar());

        // Mock data retrieval - the converter should look for these exact field names
        when(testRow.getInt("age")).thenReturn(25);
        when(testRow.getString("name")).thenReturn("John");
        when(testRow.getString("city")).thenReturn("NYC");

        // Convert using the deserialized converter
        RowData result = deserializedConverter.convert(testCassandraRow);

        assertThat(result).isNotNull();
        assertThat(result.getArity()).isEqualTo(3);
        assertThat(result.getInt(0)).isEqualTo(25); // Field 0 is "age" (IntType)
        assertThat(result.getString(1))
                .isEqualTo(StringData.fromString("John")); // Field 1 is "name" (VarCharType)
        assertThat(result.getString(2))
                .isEqualTo(StringData.fromString("NYC")); // Field 2 is "city" (VarCharType)
    }
}
