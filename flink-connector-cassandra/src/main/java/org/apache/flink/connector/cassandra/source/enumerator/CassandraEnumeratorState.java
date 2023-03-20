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

import org.apache.flink.connector.cassandra.source.split.CassandraSplit;
import org.apache.flink.connector.cassandra.source.split.SplitsGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Objects;
import java.util.Queue;

/**
 * State for {@link CassandraSplitEnumerator}. It stores the offset ({@code startToken}) of the last
 * lazy {@link CassandraSplit} generation and the number of splits left to generate. Upon
 * restoration of this sate, {@link SplitsGenerator#prepareSplits()} is obviously not re-run. So we
 * need to store also the result of this initial splits preparation ({@code increment} and {@code
 * maxToken}).
 */
public class CassandraEnumeratorState {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraEnumeratorState.class);
    private long numSplitsLeftToGenerate;
    private BigInteger increment;
    private BigInteger startToken;
    private BigInteger maxToken;
    // splits that were assigned to a failed reader and that were not part of a checkpoint, so after
    // restoration, they need to be reassigned.
    private final Queue<CassandraSplit> splitsToReassign;

    CassandraEnumeratorState() {
        this.splitsToReassign = new ArrayDeque<>();
    }

    public CassandraEnumeratorState(
            long numSplitsLeftToGenerate,
            BigInteger increment,
            BigInteger startToken,
            BigInteger maxToken,
            Queue<CassandraSplit> splitsToReassign) {
        this.numSplitsLeftToGenerate = numSplitsLeftToGenerate;
        this.increment = increment;
        this.startToken = startToken;
        this.maxToken = maxToken;
        this.splitsToReassign = splitsToReassign;
    }

    Queue<CassandraSplit> getSplitsToReassign() {
        return splitsToReassign;
    }

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

    void addSplitsBack(Collection<CassandraSplit> splits) {
        LOG.info(
                "Add {} splits back to CassandraSplitEnumerator for reassignment after failover",
                splits.size());
        splitsToReassign.addAll(splits);
    }

    /**
     * Provide a {@link CassandraSplit} that was assigned to a failed reader or lazily create one.
     * Splits contain a range of the Cassandra ring of {@code maxSplitMemorySize}. There is no way
     * to estimate the size of the data with the optional SQL filters without reading the data. So
     * the split can be smaller than {@code maxSplitMemorySize} when the query is actually executed.
     */
    public @Nullable CassandraSplit getNextSplit() {
        // serve slits to reassign first
        final CassandraSplit splitToReassign = splitsToReassign.poll();
        if (splitToReassign != null) {
            return splitToReassign;
        } // else no more splits to reassign, generate one
        if (numSplitsLeftToGenerate == 0) {
            return null; // enumerator will send the no more split message to the requesting reader
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
        if (this.splitsToReassign.size() != that.splitsToReassign.size()) {
            return false;
        }
        for (CassandraSplit cassandraSplit : splitsToReassign) {
            if (!that.splitsToReassign.contains(cassandraSplit)) {
                return false;
            }
        }
        return numSplitsLeftToGenerate == that.numSplitsLeftToGenerate
                && increment.equals(that.increment)
                && startToken.equals(that.startToken)
                && maxToken.equals(that.maxToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                numSplitsLeftToGenerate, increment, startToken, maxToken, splitsToReassign);
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
                + ", splitsToReassign="
                + splitsToReassign
                + '}';
    }
}
