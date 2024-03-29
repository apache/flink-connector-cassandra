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

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;

/**
 * Wrapper for Cassandra {@link Row} that stores {@link ExecutionInfo} Cassandra statistics about
 * the query execution that produced this row. {@link ExecutionInfo} is useful for using the
 * Cassandra mapper during row translation to pojo.
 */
public class CassandraRow {

    private final Row row;
    private final ExecutionInfo executionInfo;

    public CassandraRow(Row row, ExecutionInfo executionInfo) {
        this.row = row;
        this.executionInfo = executionInfo;
    }

    public Row getRow() {
        return row;
    }

    public ExecutionInfo getExecutionInfo() {
        return executionInfo;
    }
}
