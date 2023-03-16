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

package org.apache.flink.connector.cassandra.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;
import org.apache.flink.connector.cassandra.source.split.SplitsGenerator;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.connector.cassandra.source.split.SplitsGenerator.CassandraPartitioner.MURMUR3PARTITIONER;
import static org.apache.flink.connector.cassandra.source.split.SplitsGenerator.CassandraPartitioner.RANDOMPARTITIONER;

/** {@link SplitEnumerator} that splits Cassandra cluster into {@link CassandraSplit}s. */
public final class CassandraSplitEnumerator
        implements SplitEnumerator<CassandraSplit, CassandraEnumeratorState> {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraSplitEnumerator.class);

    private final SplitEnumeratorContext<CassandraSplit> enumeratorContext;
    private final CassandraEnumeratorState state;
    private final Cluster cluster;
    private final Long maxSplitMemorySize;
    private final Session session;
    private final String keyspace;
    private final String table;

    public CassandraSplitEnumerator(
            SplitEnumeratorContext<CassandraSplit> enumeratorContext,
            CassandraEnumeratorState state,
            ClusterBuilder clusterBuilder,
            Long maxSplitMemorySize,
            String keyspace,
            String table) {
        this.enumeratorContext = enumeratorContext;
        this.state = state == null ? new CassandraEnumeratorState() : state /* snapshot restore*/;
        this.cluster = clusterBuilder.getCluster();
        this.maxSplitMemorySize = maxSplitMemorySize;
        this.session = cluster.newSession();
        this.keyspace = keyspace;
        this.table = table;
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        checkReaderRegistered(subtaskId);
        // TODO impl lazy split generation.
        final CassandraSplit cassandraSplit = state.getASplit();
        if (cassandraSplit != null) {
            LOG.info("Assigning splits to reader {}", subtaskId);
            enumeratorContext.assignSplit(cassandraSplit, subtaskId);
        } else {
            LOG.info(
                    "No split assigned to reader {} because the enumerator has no unassigned split left. Sending NoMoreSplitsEvent to reader",
                    subtaskId);
            enumeratorContext.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void start() {
        // discover the splits and update enumerator unassigned splits.
        // There is only an initial splits discovery, no periodic discovery.
        // TODO do not discover the splits on start. Rather prepare for splits generation
        enumeratorContext.callAsync(
                this::discoverSplits,
                (splits, throwable) -> {
                    LOG.info("Add {} splits to CassandraSplitEnumerator.", splits.size());
                    state.addNewSplits(splits);
                });
    }

    private List<CassandraSplit> discoverSplits() {
        final int parallelism = enumeratorContext.currentParallelism();
        final Metadata clusterMetadata = cluster.getMetadata();
        final String partitionerName = clusterMetadata.getPartitioner();
        final SplitsGenerator.CassandraPartitioner partitioner =
                partitionerName.contains(MURMUR3PARTITIONER.getClassName())
                        ? MURMUR3PARTITIONER
                        : RANDOMPARTITIONER;
        return new SplitsGenerator(
                        partitioner, session, keyspace, table, parallelism, maxSplitMemorySize)
                .generateSplits();
    }

    @Override
    public void addSplitsBack(List<CassandraSplit> splits, int subtaskId) {
        LOG.info("Add {} splits back to CassandraSplitEnumerator.", splits.size());
        state.addNewSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    private void checkReaderRegistered(int readerId) {
        if (!enumeratorContext.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d is not registered to source coordinator", readerId));
        }
    }

    @Override
    public CassandraEnumeratorState snapshotState(long checkpointId) {
        return state;
    }

    @Override
    public void close() throws IOException {
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
