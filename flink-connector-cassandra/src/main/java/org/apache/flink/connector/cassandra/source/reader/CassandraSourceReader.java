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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;
import org.apache.flink.connector.cassandra.source.split.CassandraSplitState;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;

import java.util.Map;

/**
 * Cassandra {@link SourceReader} that reads one {@link CassandraSplit} using a single thread.
 *
 * @param <OUT> the type of elements produced by the source
 */
public class CassandraSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBase<
                CassandraRow, OUT, CassandraSplit, CassandraSplitState> {

    // TODO see if there is already and existing conf item or agree on the new name
    private static final String MAX_RECORDS_PER_SPLIT_CONF =
            "execution.source.max-records-per-split";
    private static final int MAX_RECORDS_PER_SPLIT_DEFAULT = 1000;

    public CassandraSourceReader(
            SourceReaderContext context,
            ClusterBuilder clusterBuilder,
            Class<OUT> pojoClass,
            String query,
            MapperOptions mapperOptions) {
        super(
                () -> {
                    final Configuration configuration = context.getConfiguration();
                    ConfigOption<Integer> maxRecordsPerSplit =
                            ConfigOptions.key(MAX_RECORDS_PER_SPLIT_CONF)
                                    .intType()
                                    .defaultValue(MAX_RECORDS_PER_SPLIT_DEFAULT);
                    return new CassandraSplitReader(
                            clusterBuilder, query, configuration.get(maxRecordsPerSplit));
                },
                new CassandraRecordEmitter<>(pojoClass, clusterBuilder, mapperOptions),
                context.getConfiguration(),
                context);
    }

    @Override
    protected void onSplitFinished(Map<String, CassandraSplitState> finishedSplitIds) {
        // nothing to do
    }

    @Override
    protected CassandraSplitState initializedState(CassandraSplit cassandraSplit) {
        return new CassandraSplitState(cassandraSplit);
    }

    @Override
    protected CassandraSplit toSplitType(String splitId, CassandraSplitState cassandraSplitState) {
        return cassandraSplitState.getCassandraSplit();
    }
}
