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

import java.util.Set;
import java.util.stream.Collectors;

/** Common utilities for Cassandra related tasks. */
public class CassandraUtils {

    public static String getPartitionKeyString(
            String keyspace, String table, Metadata clusterMetadata) {
        return String.join(",", getPartitionKey(keyspace, table, clusterMetadata));
    }

    public static Set<String> getPartitionKey(
            String keyspace, String table, Metadata clusterMetadata) {
        return clusterMetadata.getKeyspace(keyspace).getTable(table).getPartitionKey().stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.toSet());
    }
}
