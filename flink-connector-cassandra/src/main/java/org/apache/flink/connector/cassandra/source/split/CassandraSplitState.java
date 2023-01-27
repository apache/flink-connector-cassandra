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

package org.apache.flink.connector.cassandra.source.split;

import com.datastax.driver.core.ResultSet;

import javax.annotation.Nullable;

import java.math.BigInteger;

/**
 * Mutable {@link CassandraSplit} that keeps track of the reading process of the associated split.
 */
public class CassandraSplitState {
    private final CassandraSplit cassandraSplit;
    // Cassandra ResultSet is paginated, a new page is read only if all the records of the previous
    // one were consumed. fetch() can be interrupted so we use the resultSet to keep track of the
    // reading process.
    // It is null when reading has not started (before fetch is called on the split).
    @Nullable private ResultSet resultSet;

    public CassandraSplitState(CassandraSplit cassandraSplit) {
        this.cassandraSplit = cassandraSplit;
    }

    public CassandraSplit getCassandraSplit() {
        return cassandraSplit;
    }

    @Nullable
    public ResultSet getResultSet() {
        return resultSet;
    }

    public void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public String splitId() {
        return cassandraSplit.splitId();
    }

    public BigInteger getRingRangeStart() {
        return cassandraSplit.getRingRangeStart();
    }

    public BigInteger getRingRangeEnd() {
        return cassandraSplit.getRingRangeEnd();
    }
}
