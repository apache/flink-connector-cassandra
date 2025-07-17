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

package org.apache.flink.connector.cassandra.source.utils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.IndexMetadata;
import com.datastax.driver.core.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.connector.cassandra.source.CassandraSource.SELECT_REGEXP;
import static org.apache.flink.util.Preconditions.checkState;

/** Class to validate the user provided query. */
public class QueryValidator {
    private static final Pattern CQL_PROHIBITED_CLAUSES_REGEXP =
            Pattern.compile(
                    "(?i).*(AVG\\(|COUNT\\(|MIN\\(|MAX\\(|SUM\\(|ORDER BY\\s|GROUP BY\\s).*");
    private static final Pattern WHERE_COLUMNS_PATTERN =
            Pattern.compile("(?i)where\\s+([^;]+)", Pattern.CASE_INSENSITIVE);

    private static final Pattern COLUMN_PATTERN = Pattern.compile("(\\w+)\\s*=");

    private static final Logger LOG = LoggerFactory.getLogger(QueryValidator.class);
    private final Metadata clusterMetadata;

    public QueryValidator(ClusterBuilder clusterBuilder) {
        this.clusterMetadata = clusterBuilder.getCluster().getMetadata();
    }

    public Matcher checkQueryValidity(String query) {
        checkState(
                !query.matches(CQL_PROHIBITED_CLAUSES_REGEXP.pattern()),
                "Aggregations/OrderBy are not supported because the query is executed on subsets/partitions of the input table");
        final Matcher queryMatcher = SELECT_REGEXP.matcher(query);
        checkState(
                queryMatcher.matches(),
                "Query must be of the form select ... from keyspace.table ...;");
        String keyspace = queryMatcher.group(1);
        String table = queryMatcher.group(2);
        final Set<String> filteringColumns = extractFilteringColumns(query);

        if (!filtersOnPartitionKey(filteringColumns, keyspace, table)) {
            LOG.warn(
                    "Performance issue: the provided query does not filter on partition key forcing the cluster to read all the table partitions");
        }
        for (String column : filteringColumns) {
            if (!isIndexedColumn(column, keyspace, table)
                    && !isPrimaryKeyColumn(column, keyspace, table)) {
                LOG.warn(
                        "Performance issue: in the provided query the {} filtering column is neither indexed nor a key column leading the cluster to do a full table scan",
                        column);
            }
        }

        return queryMatcher;
    }

    @VisibleForTesting
    public boolean filtersOnPartitionKey(
            Set<String> filteringColumns, String keyspace, String table) {
        final Set<String> partitionKeyColumns =
                CassandraUtils.getPartitionKey(keyspace, table, clusterMetadata);
        return filteringColumns.containsAll(partitionKeyColumns);
    }

    @VisibleForTesting
    public boolean isIndexedColumn(String columnName, String keyspace, String table) {
        final Set<String> indexes =
                clusterMetadata.getKeyspace(keyspace).getTable(table).getIndexes().stream()
                        .map(IndexMetadata::getTarget)
                        .collect(Collectors.toSet());
        return indexes.contains(columnName);
    }

    @VisibleForTesting
    public boolean isPrimaryKeyColumn(String columnName, String keyspace, String table) {
        final Set<String> keyColumns =
                clusterMetadata.getKeyspace(keyspace).getTable(table).getPrimaryKey().stream()
                        .map(ColumnMetadata::getName)
                        .collect(Collectors.toSet());
        return keyColumns.contains(columnName);
    }

    @VisibleForTesting
    public Set<String> extractFilteringColumns(String query) {
        Matcher whereMatcher = WHERE_COLUMNS_PATTERN.matcher(query);
        Set<String> columns = new HashSet<>();
        if (whereMatcher.find()) {
            String whereClause = whereMatcher.group(1);
            Matcher colMatcher = COLUMN_PATTERN.matcher(whereClause);
            while (colMatcher.find()) {
                columns.add(colMatcher.group(1));
            }
        }
        return columns;
    }
}
