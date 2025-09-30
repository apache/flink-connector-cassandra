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

package org.apache.flink.connector.cassandra.sink.config;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * Base configuration interface for all Cassandra sink configurations.
 *
 * <p>The generic type parameter {@code <Input>} represents the type of records the sink will
 * consume. Values include:
 *
 * <ul>
 *   <li>{@link org.apache.flink.types.Row} for DataStream API jobs using Rows
 *   <li>{@link org.apache.flink.api.java.tuple.Tuple} for Tuple types
 *   <li>{@link scala.Product} for Scala case classes
 *   <li>{@link org.apache.flink.table.data.RowData} for Table/SQL API jobs
 *   <li>A user-defined POJO type for POJO sinks
 * </ul>
 *
 * @param <INPUT> the input record type that will be written to Cassandra
 */
@PublicEvolving
public interface CassandraSinkConfig<INPUT> extends Serializable {

    /**
     * Gets the record format type for this configuration.
     *
     * @return the format type used to determine the appropriate writer implementation
     */
    RecordFormatType getRecordFormatType();
}
