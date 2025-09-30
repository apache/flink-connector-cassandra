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

package org.apache.flink.connector.cassandra.sink.assembler;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.RecordFormatType;
import org.apache.flink.connector.cassandra.sink.config.RowDataSinkConfig;
import org.apache.flink.connector.cassandra.sink.planner.core.components.StatementPlanner;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/** Unit tests for {@link StaticPlannerAssembler}. */
public class StaticPlannerAssemblerTest {

    @Mock private CqlSinkConfig<String> config;
    private StaticPlannerAssembler<String> assembler;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        assembler = new StaticPlannerAssembler<>(config);
    }

    @Test
    void testAssembleWithInsertAndUpdateQueries() {
        // Test 1: INSERT query
        String insertQuery = "INSERT INTO keyspace.table (id, name) VALUES (?, ?)";
        when(config.getQuery()).thenReturn(insertQuery);
        when(config.getRecordFormatType()).thenReturn(RecordFormatType.POJO);
        when(config.getIgnoreNullFields()).thenReturn(false);

        StatementPlanner<String> insertPlanner = assembler.assemble();
        assertThat(insertPlanner).isNotNull();

        // Test 2: UPDATE query
        String updateQuery = "UPDATE keyspace.table SET name = ? WHERE id = ?";
        when(config.getQuery()).thenReturn(updateQuery);

        StatementPlanner<String> updatePlanner = assembler.assemble();
        assertThat(updatePlanner).isNotNull();
    }

    @Test
    void testAssembleWithIgnoreNullFields() {
        // Test 1: INSERT query with ignoreNullFields enabled
        String insertQuery = "INSERT INTO keyspace.table (id, name) VALUES (?, ?)";
        when(config.getQuery()).thenReturn(insertQuery);
        when(config.getRecordFormatType()).thenReturn(RecordFormatType.POJO);
        when(config.getIgnoreNullFields()).thenReturn(true);

        StatementPlanner<String> insertPlanner = assembler.assemble();
        assertThat(insertPlanner).isNotNull();

        // Test 2: UPDATE query with ignoreNullFields enabled
        String updateQuery = "UPDATE keyspace.table SET name = ? WHERE id = ?";
        when(config.getQuery()).thenReturn(updateQuery);

        StatementPlanner<String> updatePlanner = assembler.assemble();
        assertThat(updatePlanner).isNotNull();
    }

    @Test
    void testAssembleWithRowDataType() {
        // Setup for RowData type
        DataType rowDataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()));
        RowDataSinkConfig rowConfig = RowDataSinkConfig.forRowData(rowDataType);
        String insertQuery = "INSERT INTO keyspace.table (id, name) VALUES (?, ?)";
        rowConfig.withQuery(insertQuery);

        StaticPlannerAssembler<RowData> rowAssembler = new StaticPlannerAssembler<>(rowConfig);

        // Execute
        StatementPlanner<RowData> planner = rowAssembler.assemble();

        // Verify
        assertThat(planner).isNotNull();
    }

    @Test
    void testAssembleWithRowType() {
        // Setup for Row type
        CqlSinkConfig<Row> rowConfig =
                CqlSinkConfig.forRow().withQuery("INSERT INTO keyspace.table (id) VALUES (?)");

        StaticPlannerAssembler<Row> rowAssembler = new StaticPlannerAssembler<>(rowConfig);

        // Execute
        StatementPlanner<Row> planner = rowAssembler.assemble();

        // Verify
        assertThat(planner).isNotNull();
    }

    @Test
    void testAssembleWithTupleType() {
        // Setup for Tuple type
        CqlSinkConfig<Tuple> tupleConfig =
                CqlSinkConfig.forTuple()
                        .withQuery("UPDATE keyspace.table SET value = ? WHERE key = ?");

        StaticPlannerAssembler<Tuple> tupleAssembler = new StaticPlannerAssembler<>(tupleConfig);

        // Execute
        StatementPlanner<org.apache.flink.api.java.tuple.Tuple> planner = tupleAssembler.assemble();

        // Verify
        assertThat(planner).isNotNull();
    }

    @Test
    void testAssembleWithCaseVariations() {
        // Test 1: Lowercase insert
        when(config.getQuery()).thenReturn("insert into ks.tbl (id) values (?)");
        when(config.getRecordFormatType()).thenReturn(RecordFormatType.POJO);
        when(config.getIgnoreNullFields()).thenReturn(false);

        StatementPlanner<String> planner1 = assembler.assemble();
        assertThat(planner1).isNotNull();

        // Test 2: Mixed case update
        when(config.getQuery()).thenReturn("Update KS.TBL set NAME = ? where ID = ?");
        StatementPlanner<String> planner2 = assembler.assemble();
        assertThat(planner2).isNotNull();

        // Test 3: With extra spaces
        when(config.getQuery()).thenReturn("  INSERT INTO   ks.tbl   (id)   VALUES   (?)  ");
        StatementPlanner<String> planner3 = assembler.assemble();
        assertThat(planner3).isNotNull();
    }

    @Test
    void testAssembleValidationAndErrors() {
        // Test 1: Empty query throws exception
        when(config.getQuery()).thenReturn("");
        assertThatThrownBy(() -> assembler.assemble())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Query cannot be empty");

        // Test 2: Null query throws exception
        when(config.getQuery()).thenReturn(null);
        assertThatThrownBy(() -> assembler.assemble())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Query cannot be empty");

        // Test 3: SELECT query throws exception
        when(config.getQuery()).thenReturn("SELECT * FROM keyspace.table");
        assertThatThrownBy(() -> assembler.assemble())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Static mode only supports INSERT and UPDATE queries");

        // Test 4: DELETE query throws exception
        when(config.getQuery()).thenReturn("DELETE FROM keyspace.table WHERE id = ?");
        assertThatThrownBy(() -> assembler.assemble())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Static mode only supports INSERT and UPDATE queries");

        // Test 5: Invalid syntax in INSERT query
        when(config.getQuery()).thenReturn("INSERT INTO");
        when(config.getRecordFormatType()).thenReturn(RecordFormatType.POJO);
        assertThatThrownBy(() -> assembler.assemble()).isInstanceOf(IllegalArgumentException.class);

        // Test 6: Invalid syntax in UPDATE query
        when(config.getQuery()).thenReturn("UPDATE table");
        assertThatThrownBy(() -> assembler.assemble()).isInstanceOf(IllegalArgumentException.class);

        // Test 7: Null config in constructor is allowed but fails on assemble
        assertThatThrownBy(() -> new StaticPlannerAssembler<>(null).assemble())
                .isInstanceOf(NullPointerException.class);
    }
}
