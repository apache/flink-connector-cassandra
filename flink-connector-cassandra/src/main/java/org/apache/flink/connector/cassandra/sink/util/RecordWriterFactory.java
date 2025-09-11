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

package org.apache.flink.connector.cassandra.sink.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.cassandra.sink.config.CassandraSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.CqlSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.PojoSinkConfig;
import org.apache.flink.connector.cassandra.sink.config.RecordFormatType;
import org.apache.flink.connector.cassandra.sink.writer.CassandraRecordWriter;
import org.apache.flink.connector.cassandra.sink.writer.CqlRecordWriter;
import org.apache.flink.connector.cassandra.sink.writer.PojoRecordWriter;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

/**
 * Factory for creating appropriate {@link CassandraRecordWriter} instances based on the sink
 * configuration's record format type.
 *
 * <p>This factory uses the configuration's {@link RecordFormatType} to dispatch to the correct
 * writer implementation without requiring runtime type checks or visitor patterns.
 *
 * <p>Supported mappings:
 *
 * <ul>
 *   <li>{@link RecordFormatType#POJO} → {@link PojoRecordWriter} (uses DataStax Mapper)
 *   <li>{@link RecordFormatType#TUPLE} → {@link CqlRecordWriter} (structured data)
 *   <li>{@link RecordFormatType#ROW} → {@link CqlRecordWriter} (structured data)
 *   <li>{@link RecordFormatType#ROWDATA} → {@link CqlRecordWriter} (optimized for Table API)
 *   <li>{@link RecordFormatType#SCALA_PRODUCT} → {@link CqlRecordWriter} (Scala types)
 * </ul>
 */
@Internal
public final class RecordWriterFactory {

    /**
     * Creates the appropriate record writer for the given configuration.
     *
     * @param clusterBuilder the cluster builder for Cassandra connection
     * @param config the sink configuration
     * @param <T> the input record type
     * @return a record writer appropriate for the configuration's format type
     * @throws IllegalArgumentException if the format type is not supported
     */
    public static <T> CassandraRecordWriter<T> create(
            ClusterBuilder clusterBuilder, CassandraSinkConfig<T> config) {
        RecordFormatType formatType = config.getRecordFormatType();
        switch (formatType) {
            case POJO:
                return new PojoRecordWriter<>(clusterBuilder, (PojoSinkConfig<T>) config);
            case TUPLE:
            case ROW:
            case ROWDATA:
            case SCALA_PRODUCT:
                return new CqlRecordWriter<>(clusterBuilder, (CqlSinkConfig<T>) config);
            default:
                throw new IllegalArgumentException("Unsupported format type: " + formatType);
        }
    }
}
