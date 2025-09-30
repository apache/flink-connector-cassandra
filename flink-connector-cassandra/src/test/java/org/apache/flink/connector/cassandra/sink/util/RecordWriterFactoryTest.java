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

package org.apache.flink.connector.cassandra.sink.util;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.connector.cassandra.sink.config.CassandraSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.RowDataSinkConfig;
import org.apache.flink.connector.cassandra.sink.writer.CassandraRecordWriter;
import org.apache.flink.connector.cassandra.sink.writer.CqlRecordWriter;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import com.datastax.driver.core.Cluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for {@link RecordWriterFactory}. */
public class RecordWriterFactoryTest {

    @Mock private ClusterBuilder clusterBuilder;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        // Setup clusterBuilder to return a valid cluster
        when(clusterBuilder.getCluster()).thenReturn(mock(Cluster.class));
    }

    @Test
    void testVariousFormatsReturnCqlRecordWriter() {
        // Test ROW format returns CqlRecordWriter
        CqlSinkConfig<Row> rowConfig =
                CqlSinkConfig.forRow().withQuery("INSERT INTO ks.tbl (id) VALUES (?)");
        CassandraRecordWriter<Row> rowWriter =
                RecordWriterFactory.create(clusterBuilder, rowConfig);
        assertThat(rowWriter).isInstanceOf(CqlRecordWriter.class);
        assertThat(rowWriter).isExactlyInstanceOf(CqlRecordWriter.class);

        // Test TUPLE format returns CqlRecordWriter
        CqlSinkConfig<Tuple> tupleConfig =
                CqlSinkConfig.forTuple().withQuery("INSERT INTO ks.tbl (id) VALUES (?)");
        CassandraRecordWriter<Tuple> tupleWriter =
                RecordWriterFactory.create(clusterBuilder, tupleConfig);
        assertThat(tupleWriter).isInstanceOf(CqlRecordWriter.class);
        assertThat(tupleWriter).isExactlyInstanceOf(CqlRecordWriter.class);

        // Test ROWDATA format returns CqlRecordWriter
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("id", new IntType()),
                                new RowType.RowField("name", new VarCharType())));
        DataType dataType = TypeConversions.fromLogicalToDataType(rowType);
        RowDataSinkConfig rowDataConfig =
                RowDataSinkConfig.forRowData(dataType)
                        .withQuery("INSERT INTO ks.tbl (id, name) VALUES (?, ?)");
        CassandraRecordWriter<RowData> rowDataWriter =
                RecordWriterFactory.create(clusterBuilder, rowDataConfig);
        assertThat(rowDataWriter).isInstanceOf(CqlRecordWriter.class);
        assertThat(rowDataWriter).isExactlyInstanceOf(CqlRecordWriter.class);

        // Test SCALA_PRODUCT format returns CqlRecordWriter
        CqlSinkConfig<scala.Product> scalaConfig =
                CqlSinkConfig.forScalaProduct().withQuery("INSERT INTO ks.tbl (id) VALUES (?)");
        CassandraRecordWriter<scala.Product> scalaWriter =
                RecordWriterFactory.create(clusterBuilder, scalaConfig);
        assertThat(scalaWriter).isInstanceOf(CqlRecordWriter.class);
        assertThat(scalaWriter).isExactlyInstanceOf(CqlRecordWriter.class);
    }

    @Test
    void testUnsupportedFormatTypeThrowsNullPointerException() {
        // Create a mock config that returns null format type
        CassandraSinkConfig<?> config = mock(CassandraSinkConfig.class);
        when(config.getRecordFormatType()).thenReturn(null);

        // The factory will throw NPE when trying to switch on null
        assertThatThrownBy(() -> RecordWriterFactory.create(clusterBuilder, config))
                .isInstanceOf(NullPointerException.class);
    }
}
