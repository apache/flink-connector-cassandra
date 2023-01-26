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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * {@link SplitReader} for Cassandra source. This class is responsible for fetching the records as
 * {@link CassandraRow}s. For that, it executes a range query (query that outputs records belonging
 * to Cassandra token range) based on the user specified query. This class manages the Cassandra
 * cluster and session.
 */
public class CassandraSplitReader implements SplitReader<CassandraRow, CassandraSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraSplitReader.class);
    public static final String SELECT_REGEXP = "(?i)select .+ from (\\w+)\\.(\\w+).*;$";

    private final Cluster cluster;
    private final Session session;
    private final Set<CassandraSplit> unprocessedSplits;
    private final AtomicBoolean wakeup = new AtomicBoolean(false);
    private final String query;

    public CassandraSplitReader(ClusterBuilder clusterBuilder, String query) {
        this.unprocessedSplits = new HashSet<>();
        this.query = query;
        cluster = clusterBuilder.getCluster();
        session = cluster.connect();
    }

    @Override
    public RecordsWithSplitIds<CassandraRow> fetch() {
        Map<String, Collection<CassandraRow>> recordsBySplit = new HashMap<>();
        Set<String> finishedSplits = new HashSet<>();

        Metadata clusterMetadata = cluster.getMetadata();
        String partitionKey = getPartitionKey(clusterMetadata);
        String finalQuery = generateRangeQuery(query, partitionKey);
        PreparedStatement preparedStatement = session.prepare(finalQuery);

        // Set wakeup to false to start consuming
        wakeup.compareAndSet(true, false);
        for (CassandraSplit cassandraSplit : unprocessedSplits) {
            // allow to interrupt the reading of splits especially the blocking session.execute()
            // call) as requested in the API
            if (wakeup.get()) {
                break;
            }
            try {
                final String cassandraSplitId = cassandraSplit.splitId();
                Token startToken =
                        clusterMetadata.newToken(cassandraSplit.getRingRangeStart().toString());
                Token endToken =
                        clusterMetadata.newToken(cassandraSplit.getRingRangeEnd().toString());
                final ResultSet resultSet =
                        session.execute(
                                preparedStatement
                                        .bind()
                                        .setToken(0, startToken)
                                        .setToken(1, endToken));
                addRecordsToOutput(resultSet, recordsBySplit, cassandraSplitId);
                // put the already read split to finished splits
                finishedSplits.add(cassandraSplitId);
                // for reentrant calls: if fetch is woken up,
                // do not reprocess the already processed splits
                unprocessedSplits.remove(cassandraSplit);
            } catch (Exception ex) {
                LOG.error("Error while reading split ", ex);
            }
        }
        return new RecordsBySplits<>(recordsBySplit, finishedSplits);
    }

    private String getPartitionKey(Metadata clusterMetadata) {
        Matcher queryMatcher = Pattern.compile(SELECT_REGEXP).matcher(query);
        if (!queryMatcher.matches()) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to extract keyspace and table out of the provided query: %s",
                            query));
        }
        String keyspace = queryMatcher.group(1);
        String table = queryMatcher.group(2);
        return clusterMetadata.getKeyspace(keyspace).getTable(table).getPartitionKey().stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.joining(","));
    }

    @Override
    public void wakeUp() {
        wakeup.compareAndSet(false, true);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<CassandraSplit> splitsChanges) {
        unprocessedSplits.addAll(splitsChanges.splits());
    }

    /**
     * Utility method to add the ring token filtering clauses to the user query to generate the
     * split query. For example:
     *
     * <ul>
     *   <li><code>"select * from
     *       keyspace.table where field1=value1;"</code> will be transformed into <code>
     *       "select * from
     *       keyspace.table where (token(partitionKey) >= ?) AND (token(partitionKey) < ?) AND
     *       field1=value1;"</code>
     *   <li><code>"select * from
     *       keyspace.table;"</code> will be transformed into <code>
     *       "select * from keyspace.table WHERE
     *       (token(%s) >= ?) AND (token(%s) < ?);"</code>
     * </ul>
     *
     * @param query the user input query
     * @param partitionKey Cassandra partition key of the user provided table
     * @return the final split query that will be sent to the Cassandra cluster
     */
    @VisibleForTesting
    static String generateRangeQuery(String query, String partitionKey) {
        Matcher queryMatcher = Pattern.compile(SELECT_REGEXP).matcher(query);
        if (!queryMatcher.matches()) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to extract keyspace and table out of the provided query: %s",
                            query));
        }
        final int whereIndex = query.toLowerCase().indexOf("where");
        int insertionPoint;
        String filter;
        if (whereIndex != -1) {
            insertionPoint = whereIndex + "where".length();
            filter =
                    String.format(
                            " (token(%s) >= ?) AND (token(%s) < ?) AND",
                            partitionKey, partitionKey);
        } else {
            // end of keyspace.table
            insertionPoint = queryMatcher.end(2);
            filter =
                    String.format(
                            " WHERE (token(%s) >= ?) AND (token(%s) < ?)",
                            partitionKey, partitionKey);
        }
        return String.format(
                "%s%s%s",
                query.substring(0, insertionPoint), filter, query.substring(insertionPoint));
    }

    /**
     * This method populates the {@code Map<String, Collection<CassandraRow>> recordsBySplit} map
     * that is used to create the {@link RecordsBySplits} that are output by the fetch method. It
     * modifies its {@code output} parameter.
     */
    private void addRecordsToOutput(
            ResultSet resultSet,
            Map<String, Collection<CassandraRow>> output,
            String cassandraSplitId) {
        resultSet.forEach(
                row ->
                        output.computeIfAbsent(cassandraSplitId, id -> new ArrayList<>())
                                .add(new CassandraRow(row, resultSet.getExecutionInfo())));
    }

    @Override
    public void close() throws Exception {
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
