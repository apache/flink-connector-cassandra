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

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

import java.util.List;
import java.util.stream.Collectors;

/** Common utilities for Cassandra related tasks. */
public class CassandraUtils {

    /**
     * Returns the partition key columns formatted to be used in a CQL query, in the order of the
     * partition key. The names of the case-sensitive columns are double-quoted.
     */
    public static String getPartitionKeyString(
            String keyspace, String table, Metadata clusterMetadata) {
        return getTable(keyspace, table, clusterMetadata).getPartitionKey().stream()
                .map(column -> column.getName().asCql(true))
                .collect(Collectors.joining(","));
    }

    /** Returns the names of the partition key columns, in the order of the partition key. */
    public static List<String> getPartitionKey(
            String keyspace, String table, Metadata clusterMetadata) {
        return getTable(keyspace, table, clusterMetadata).getPartitionKey().stream()
                .map(column -> column.getName().asInternal())
                .collect(Collectors.toList());
    }

    /**
     * Returns the metadata of a table.
     *
     * @throws IllegalArgumentException if the keyspace or the table does not exist
     */
    public static TableMetadata getTable(String keyspace, String table, Metadata clusterMetadata) {
        return clusterMetadata
                .getKeyspace(keyspace)
                .flatMap(keyspaceMetadata -> keyspaceMetadata.getTable(table))
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "Table %s.%s does not exist.", keyspace, table)));
    }
}
