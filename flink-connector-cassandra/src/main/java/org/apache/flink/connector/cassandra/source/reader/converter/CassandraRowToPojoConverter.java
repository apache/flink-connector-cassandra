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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.cassandra.source.reader.CassandraRow;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

/**
 * Cassandra row converter using Cassandra's POJO mapper to convert Cassandra's {@link Row} to POJO.
 *
 * @param <OUT> The POJO type
 */
@Internal
public class CassandraRowToPojoConverter<OUT> implements CassandraRowToTypeConverter<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraRowToPojoConverter.class);

    private static final long serialVersionUID = 1L;

    private final Class<OUT> pojoClass;
    private final MapperOptions mapperOptions;
    private final ClusterBuilder clusterBuilder;

    private transient Mapper<OUT> mapper;

    public CassandraRowToPojoConverter(
            Class<OUT> pojoClass, MapperOptions mapperOptions, ClusterBuilder clusterBuilder) {
        this.pojoClass = requireNonNull(pojoClass, "POJO class cannot be null");
        this.mapperOptions = requireNonNull(mapperOptions, "Mapper options cannot be null");
        this.clusterBuilder = requireNonNull(clusterBuilder, "Cluster builder cannot be null");
    }

    @Override
    public OUT convert(CassandraRow cassandraRow) {
        if (mapper == null) {
            initializeMapper();
        }
        // Use the SingleRowResultSet wrapper to adapt Row to ResultSet with proper ExecutionInfo
        return mapper.map(
                        new SingleRowResultSet(
                                cassandraRow.getRow(), cassandraRow.getExecutionInfo()))
                .one();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return TypeInformation.of(pojoClass);
    }

    private void initializeMapper() {
        Cluster cluster = clusterBuilder.getCluster();
        Session session = cluster.connect();
        this.mapper = new MappingManager(session).mapper(pojoClass);
        Mapper.Option[] optionsArray = mapperOptions.getMapperOptions();
        if (optionsArray != null) {
            mapper.setDefaultGetOptions(optionsArray);
        }
    }

    /**
     * A ResultSet implementation that wraps a single Row. This is needed because Cassandra's mapper
     * API expects a ResultSet.
     */
    private static class SingleRowResultSet implements ResultSet {
        private final Row row;
        private final ExecutionInfo executionInfo;

        SingleRowResultSet(Row row, ExecutionInfo executionInfo) {
            this.row = row;
            this.executionInfo = executionInfo;
        }

        @Override
        public Row one() {
            return row;
        }

        @Override
        public List<Row> all() {
            return singletonList(row);
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
        public ExecutionInfo getExecutionInfo() {
            return executionInfo;
        }

        @Override
        public List<ExecutionInfo> getAllExecutionInfo() {
            return singletonList(executionInfo);
        }

        @Override
        public boolean wasApplied() {
            return true;
        }

        @Override
        public ColumnDefinitions getColumnDefinitions() {
            return row.getColumnDefinitions();
        }
    }
}
