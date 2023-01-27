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
import org.apache.flink.connector.cassandra.source.split.CassandraSplitState;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
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
    private final Set<CassandraSplitState> unprocessedSplits;
    private final AtomicBoolean wakeup = new AtomicBoolean(false);
    private final String query;
    private final int maxRecordsPerSplit;

    public CassandraSplitReader(
            ClusterBuilder clusterBuilder, String query, int maxRecordsPerSplit) {
        this.unprocessedSplits = new HashSet<>();
        this.query = query;
        this.maxRecordsPerSplit = maxRecordsPerSplit;
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
        for (CassandraSplitState cassandraSplitState : unprocessedSplits) {
            // allow to interrupt the reading of splits especially the blocking session.execute()
            // call) as requested in the API
            if (wakeup.get()) {
                break;
            }
            try {
                // TODO add a test for resume of fetch()
                if (cassandraSplitState.getResultSet() != null) { // resumed fetch()
                    // add the records already contained in cassandraSplitState#resultSet
                    addRecordsToOutput(null, cassandraSplitState, recordsBySplit);
                } else { // first time we read this split
                    Token startToken =
                            clusterMetadata.newToken(
                                    cassandraSplitState.getRingRangeStart().toString());
                    Token endToken =
                            clusterMetadata.newToken(
                                    cassandraSplitState.getRingRangeEnd().toString());
                    final ResultSet resultSet =
                            session.execute(
                                    preparedStatement
                                            .bind()
                                            .setToken(0, startToken)
                                            .setToken(1, endToken));
                    addRecordsToOutput(resultSet, cassandraSplitState, recordsBySplit);
                }
                final String cassandraSplitId = cassandraSplitState.splitId();
                // add the already read (or even empty) split to finished splits
                finishedSplits.add(cassandraSplitId);
                // for reentrant calls: if fetch is restarted,
                // do not reprocess the already processed splits
                unprocessedSplits.remove(cassandraSplitState);
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
        for (CassandraSplit cassandraSplit : splitsChanges.splits()) {
            unprocessedSplits.add(new CassandraSplitState(cassandraSplit));
        }
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
     * modifies its {@code output} parameter and updates {@link CassandraSplitState} to keep track
     * of the output.
     */
    private void addRecordsToOutput(
            ResultSet resultSet,
            CassandraSplitState cassandraSplitState,
            Map<String, Collection<CassandraRow>> output) {
        ResultSet finalResultSet;
        if (resultSet == null) { // resumed fetch()
            assert cassandraSplitState.getResultSet() != null;
            finalResultSet = cassandraSplitState.getResultSet();
        } else { // output the result of the query
            finalResultSet = resultSet;
            // keep track of where we are in the resultset
            cassandraSplitState.setResultSet(finalResultSet);
        }
        // output the rows in the ResultSet until no more rows or maxRecordsPerSplit is met
        int nbRecords = 0;
        while (nbRecords < maxRecordsPerSplit && !finalResultSet.isExhausted()) {
            final Row row = finalResultSet.one();
            output.computeIfAbsent(cassandraSplitState.splitId(), id -> new ArrayList<>())
                    .add(new CassandraRow(row, finalResultSet.getExecutionInfo()));
            nbRecords++;
        }
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
