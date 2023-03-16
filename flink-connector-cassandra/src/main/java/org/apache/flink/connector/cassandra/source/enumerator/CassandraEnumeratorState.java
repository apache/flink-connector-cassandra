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

package org.apache.flink.connector.cassandra.source.enumerator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;

import javax.annotation.Nullable;

import java.math.BigInteger;
import java.util.Objects;

/** State for {@link CassandraSplitEnumerator} to track the splits yet to assign. */
public class CassandraEnumeratorState {
    private long numSplitsToGenerate;
    private BigInteger increment;
    private BigInteger startToken;
    private BigInteger endToken;
    private BigInteger maxToken;

    @VisibleForTesting
    public CassandraEnumeratorState() {}

    // constructor for serde
    CassandraEnumeratorState(
            long numSplitsToGenerate,
            BigInteger increment,
            BigInteger startToken,
            BigInteger endToken,
            BigInteger maxToken) {
        this.numSplitsToGenerate = numSplitsToGenerate;
        this.increment = increment;
        this.startToken = startToken;
        this.endToken = endToken;
        this.maxToken = maxToken;
    }

    public void initializeState(
            long numSplitsToGenerate,
            BigInteger increment,
            BigInteger startToken,
            BigInteger maxToken) {
        this.numSplitsToGenerate = numSplitsToGenerate;
        this.increment = increment;
        this.startToken = startToken;
        this.maxToken = maxToken;
    }

    @VisibleForTesting
    public long getNumSplitsToGenerate() {
        return numSplitsToGenerate;
    }

    /**
     * Lazily create a {@link CassandraSplit} containing a range of the Cassandra ring of {@code
     * maxSplitMemorySize}.
     */
    public @Nullable CassandraSplit getNextSplit() {
        if (numSplitsToGenerate == 0) {
            return null; // no more splits
        }
        endToken =
                numSplitsToGenerate == 1
                        // last split to generate, round up to the last token of the ring
                        ? maxToken
                        : startToken.add(increment);
        // prepare for next call
        BigInteger startToken = this.startToken;
        this.startToken = endToken;
        numSplitsToGenerate--;
        return new CassandraSplit(startToken, endToken);
    }

    // TODO update serialization and equals/hashcode
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraEnumeratorState that = (CassandraEnumeratorState) o;
        if (this.unassignedSplits.size() != that.unassignedSplits.size()) {
            return false;
        }
        for (CassandraSplit cassandraSplit : unassignedSplits) {
            if (!that.unassignedSplits.contains(cassandraSplit)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(unassignedSplits);
    }
}
