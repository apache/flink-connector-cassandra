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

    public CassandraSplitEnumerator(
            SplitEnumeratorContext<CassandraSplit> enumeratorContext,
            CassandraEnumeratorState state,
            ClusterBuilder clusterBuilder) {
        this.enumeratorContext = enumeratorContext;
        this.state = state == null ? new CassandraEnumeratorState() : state /* snapshot restore*/;
        this.cluster = clusterBuilder.getCluster();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        assignUnprocessedSplitToReader(subtaskId);
    }

    @Override
    public void start() {
        // discover the splits and update unprocessed splits and then assign them.
        // There is only an initial splits discovery, no periodic discovery.
        enumeratorContext.callAsync(
                this::discoverSplits,
                (splits, throwable) -> {
                    LOG.info("Add {} splits to CassandraSplitEnumerator.", splits.size());
                    state.addNewSplits(splits);
                });
    }

    private List<CassandraSplit> discoverSplits() {
        final int numberOfSplits = enumeratorContext.currentParallelism();
        final Metadata clusterMetadata = cluster.getMetadata();
        final String partitionerName = clusterMetadata.getPartitioner();
        final SplitsGenerator.CassandraPartitioner partitioner =
                partitionerName.contains(MURMUR3PARTITIONER.className())
                        ? MURMUR3PARTITIONER
                        : RANDOMPARTITIONER;
        return new SplitsGenerator(partitioner).generateSplits(numberOfSplits);
    }

    @Override
    public void addSplitsBack(List<CassandraSplit> splits, int subtaskId) {
        LOG.info("Add {} splits back to CassandraSplitEnumerator.", splits.size());
        state.addNewSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Adding reader {} to CassandraSplitEnumerator.", subtaskId);
        assignUnprocessedSplitToReader(subtaskId);
    }

    private void assignUnprocessedSplitToReader(int readerId) {
        checkReaderRegistered(readerId);
        final CassandraSplit cassandraSplit = state.getASplit();
        if (cassandraSplit != null) {
            LOG.info("Assigning splits to reader {}", readerId);
            enumeratorContext.assignSplit(cassandraSplit, readerId);
        } else {
            LOG.info(
                    "No split assigned to reader {} because the enumerator has no unassigned split left",
                    readerId);
        }
        if (!state.hasMoreSplits()) {
            LOG.info(
                    "No more CassandraSplits to assign. Sending NoMoreSplitsEvent to reader {}.",
                    readerId);
            enumeratorContext.signalNoMoreSplits(readerId);
        }
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
        cluster.close();
    }
}
