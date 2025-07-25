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

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.cassandra.source.utils.CassandraUtils;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

/**
 * Factory to create {@link CassandraSourceReader}s and allow sharing the cluster and the session
 * objects.
 */
public class CassandraSourceReaderFactory<OUT> {
    public CassandraSourceReader<OUT> create(
            SourceReaderContext context,
            ClusterBuilder clusterBuilder,
            Class<OUT> pojoClass,
            String query,
            String keyspace,
            String table,
            MapperOptions mapperOptions) {
        Cluster cluster = clusterBuilder.getCluster();
        Session session = cluster.connect();
        Mapper<OUT> mapper = new MappingManager(session).mapper(pojoClass);
        if (mapperOptions != null) {
            Mapper.Option[] optionsArray = mapperOptions.getMapperOptions();
            if (optionsArray != null) {
                mapper.setDefaultGetOptions(optionsArray);
            }
        }
        final String partitionKey =
                CassandraUtils.getPartitionKeyString(keyspace, table, cluster.getMetadata());
        return new CassandraSourceReader<>(context, query, partitionKey, cluster, session, mapper);
    }
}
