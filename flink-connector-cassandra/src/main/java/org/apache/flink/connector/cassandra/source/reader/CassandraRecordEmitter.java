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
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * {@link RecordEmitter} that converts the {@link CassandraRow} read by the {@link
 * CassandraSplitReader} to specified POJO and output it. This class uses the Cassandra driver
 * mapper to map the row to the POJO.
 *
 * @param <OUT> type of POJO record to output
 */
class CassandraRecordEmitter<OUT> implements RecordEmitter<CassandraRow, OUT, CassandraSplit> {

    private final Function<ResultSet, OUT> map;

    public CassandraRecordEmitter(Function<ResultSet, OUT> map) {
        this.map = map;
    }

    @Override
    public void emitRecord(
            CassandraRow cassandraRow, SourceOutput<OUT> output, CassandraSplit cassandraSplit) {
        // Mapping from a row to a Class<OUT> is a complex operation involving reflection API.
        // It is better to use Cassandra mapper for it.
        // But the mapper takes only a resultSet as input hence forging one containing only the Row
        ResultSet resultSet = new SingleRowResultSet(cassandraRow);
        // output the pojo based on the cassandraRow
        output.collect(map.apply(resultSet));
    }

    private static class SingleRowResultSet implements ResultSet {
        private final CassandraRow cassandraRow;
        private final Row row;

        private SingleRowResultSet(CassandraRow cassandraRow) {
            this.cassandraRow = cassandraRow;
            this.row = cassandraRow.getRow();
        }

        @Override
        public Row one() {
            return row;
        }

        @Override
        public ColumnDefinitions getColumnDefinitions() {
            return row.getColumnDefinitions();
        }

        @Override
        public boolean wasApplied() {
            return true;
        }

        @Override
        public boolean isExhausted() {
            return true;
        }

        @Override
        public boolean isFullyFetched() {
            return true;
        }

        @Override
        public int getAvailableWithoutFetching() {
            return 1;
        }

        @Override
        public ListenableFuture<ResultSet> fetchMoreResults() {
            return Futures.immediateFuture(null);
        }

        @Override
        public List<Row> all() {
            return Collections.singletonList(row);
        }

        @Override
        public Iterator<Row> iterator() {
            return new Iterator<Row>() {

                @Override
                public boolean hasNext() {
                    return true;
                }

                @Override
                public Row next() {
                    return row;
                }
            };
        }

        @Override
        public ExecutionInfo getExecutionInfo() {
            return cassandraRow.getExecutionInfo();
        }

        @Override
        public List<ExecutionInfo> getAllExecutionInfo() {
            return Collections.singletonList(cassandraRow.getExecutionInfo());
        }
    }
}
