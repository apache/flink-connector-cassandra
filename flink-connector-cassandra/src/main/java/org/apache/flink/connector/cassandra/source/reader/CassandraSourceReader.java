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

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Cassandra {@link SourceReader} that reads one {@link CassandraSplit} using a single thread.
 *
 * @param <OUT> the type of elements produced by the source
 */
class CassandraSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBase<
                CassandraRow, OUT, CassandraSplit, CassandraSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraSourceReader.class);

    private final Cluster cluster;
    private final Session session;

    // created by the factory
    CassandraSourceReader(
            SourceReaderContext context,
            String query,
            String keyspace,
            String table,
            Cluster cluster,
            Session session,
            Mapper<OUT> mapper) {
        super(
                () -> new CassandraSplitReader(cluster, session, query, keyspace, table),
                new CassandraRecordEmitter<>(resultSet -> mapper.map(resultSet).one()),
                context.getConfiguration(),
                context);
        this.cluster = cluster;
        this.session = session;
    }

    @Override
    public void start() {
        context.sendSplitRequest();
    }

    @Override
    protected void onSplitFinished(Map<String, CassandraSplit> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    protected CassandraSplit initializedState(CassandraSplit cassandraSplit) {
        return cassandraSplit;
    }

    @Override
    protected CassandraSplit toSplitType(String splitId, CassandraSplit cassandraSplit) {
        return cassandraSplit;
    }

    @Override
    public void close() throws Exception {
        super.close();
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            LOG.error("Error while closing session.", e);
        }
        try {
            if (cluster != null) {
                cluster.close();
            }
        } catch (Exception e) {
            LOG.error("Error while closing cluster.", e);
        }
    }
}
