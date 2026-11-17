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

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Metadata;

import java.util.List;
import java.util.stream.Collectors;

/** Common utilities for Cassandra related tasks. */
public class CassandraUtils {

    /**
     * Returns the partition key columns as a CQL list for {@code token(...)}: in partition key
     * order, and quoted where the column names need it.
     */
    public static String getPartitionKeyString(
            String keyspace, String table, Metadata clusterMetadata) {
        return getPartitionKey(keyspace, table, clusterMetadata).stream()
                .map(Metadata::quoteIfNecessary)
                .collect(Collectors.joining(","));
    }

    /** Returns the partition key column names in partition key order. */
    public static List<String> getPartitionKey(
            String keyspace, String table, Metadata clusterMetadata) {
        return clusterMetadata.getKeyspace(keyspace).getTable(table).getPartitionKey().stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.toList());
    }
}
