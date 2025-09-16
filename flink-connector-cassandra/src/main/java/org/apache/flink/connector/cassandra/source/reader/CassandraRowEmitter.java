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

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.cassandra.source.reader.converter.CassandraRowToTypeConverter;
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Unified record emitter that uses the strategy pattern for deserialization.
 *
 * @param <OUT> The output type
 */
class CassandraRowEmitter<OUT> implements RecordEmitter<CassandraRow, OUT, CassandraSplit> {

    private final CassandraRowToTypeConverter<OUT> converter;

    public CassandraRowEmitter(CassandraRowToTypeConverter<OUT> converter) {
        this.converter = checkNotNull(converter, "Converter cannot be null");
    }

    @Override
    public void emitRecord(
            CassandraRow element, SourceOutput<OUT> output, CassandraSplit splitState) {
        try {
            OUT record = converter.convert(element);
            output.collect(record);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize Cassandra row", e);
        }
    }
}
