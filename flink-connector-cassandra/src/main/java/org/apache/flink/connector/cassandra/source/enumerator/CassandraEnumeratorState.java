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
import org.apache.flink.connector.cassandra.source.split.SplitsGenerator;

import javax.annotation.Nullable;

import java.math.BigInteger;
import java.util.Objects;

/**
 * State for {@link CassandraSplitEnumerator}. It stores the offset ({@code startToken}) of the last
 * lazy {@link CassandraSplit} generation and the number of splits left to generate. Upon
 * restoration of this sate, {@link SplitsGenerator#prepareSplits()} is obviously not re-run. So we
 * need to store also the result of this initial splits preparation ({@code increment} and {@code
 * maxToken}).
 */
public class CassandraEnumeratorState {
    private long numSplitsLeftToGenerate;
    private BigInteger increment;
    private BigInteger startToken;
    private BigInteger maxToken;

    CassandraEnumeratorState() {}

    public CassandraEnumeratorState(
            long numSplitsLeftToGenerate,
            BigInteger increment,
            BigInteger startToken,
            BigInteger maxToken) {
        this.numSplitsLeftToGenerate = numSplitsLeftToGenerate;
        this.increment = increment;
        this.startToken = startToken;
        this.maxToken = maxToken;
    }

    @VisibleForTesting
    public long getNumSplitsLeftToGenerate() {
        return numSplitsLeftToGenerate;
    }

    BigInteger getIncrement() {
        return increment;
    }

    BigInteger getStartToken() {
        return startToken;
    }

    BigInteger getMaxToken() {
        return maxToken;
    }

    /**
     * Lazily create a {@link CassandraSplit} containing a range of the Cassandra ring of {@code
     * maxSplitMemorySize}.
     */
    public @Nullable CassandraSplit getNextSplit() {
        if (numSplitsLeftToGenerate == 0) {
            return null; // enumerator will send the no more split message
        }
        BigInteger endToken =
                numSplitsLeftToGenerate == 1
                        // last split to generate, round up to the last token of the ring
                        ? maxToken
                        : startToken.add(increment);
        CassandraSplit split = new CassandraSplit(startToken, endToken);
        // prepare for next call
        this.startToken = endToken;
        numSplitsLeftToGenerate--;
        return split;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraEnumeratorState that = (CassandraEnumeratorState) o;
        return numSplitsLeftToGenerate == that.numSplitsLeftToGenerate
                && increment.equals(that.increment)
                && startToken.equals(that.startToken)
                && maxToken.equals(that.maxToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numSplitsLeftToGenerate, increment, startToken, maxToken);
    }

    @Override
    public String toString() {
        return "CassandraEnumeratorState{"
                + "numSplitsLeftToGenerate="
                + numSplitsLeftToGenerate
                + ", increment="
                + increment
                + ", startToken="
                + startToken
                + ", maxToken="
                + maxToken
                + '}';
    }
}
