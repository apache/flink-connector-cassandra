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

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.math.BigInteger;

/**
 * Immutable {@link SourceSplit} for Cassandra source. A Cassandra split is a slice of the Cassandra
 * tokens ring (i.e. a ringRange).
 */
public class CassandraSplit implements SourceSplit, Serializable {

    private final BigInteger ringRangeStart;
    private final BigInteger ringRangeEnd;

    public CassandraSplit(BigInteger ringRangeStart, BigInteger ringRangeEnd) {
        this.ringRangeStart = ringRangeStart;
        this.ringRangeEnd = ringRangeEnd;
    }

    public BigInteger getRingRangeStart() {
        return ringRangeStart;
    }

    public BigInteger getRingRangeEnd() {
        return ringRangeEnd;
    }

    @Override
    public String splitId() {
        return String.format("(%s,%s)", ringRangeStart.toString(), ringRangeEnd.toString());
    }

    @Override
    public String toString() {
        return splitId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraSplit other = (CassandraSplit) o;
        return ringRangeStart.equals(other.ringRangeStart)
                && ringRangeEnd.equals(other.ringRangeEnd);
    }

    @Override
    public int hashCode() {
        return 31 * ringRangeStart.hashCode() + ringRangeEnd.hashCode();
    }
}
