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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * This class generates {@link CassandraSplit}s by generating {@link RingRange}s based on Cassandra
 * cluster partitioner and Flink source parallelism.
 */
public final class SplitsGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(SplitsGenerator.class);

    private final CassandraPartitioner partitioner;

    public SplitsGenerator(CassandraPartitioner partitioner) {
        this.partitioner = partitioner;
    }

    /**
     * Given properly ordered list of Cassandra tokens, compute at least {@code totalSplitCount}
     * splits. Each split can contain several token ranges in order to reduce the overhead of
     * Cassandra vnodes. Currently, token range grouping is not smart and doesn't check if they
     * share the same replicas.
     *
     * @param totalSplitCount requested total amount of splits. This function may generate more
     *     splits.
     * @param ringTokens list of all start tokens in Cassandra cluster. They have to be in ring
     *     order.
     * @return list containing at least {@code totalSplitCount} CassandraSplits.
     */
    public List<CassandraSplit> generateSplits(long totalSplitCount, List<BigInteger> ringTokens) {
        if (totalSplitCount == 1) {
            RingRange totalRingRange = RingRange.of(partitioner.min(), partitioner.max());
            return Collections.singletonList(
                    new CassandraSplit(Collections.singleton(totalRingRange)));
        }
        int tokenRangeCount = ringTokens.size();

        List<RingRange> ringRanges = new ArrayList<>();
        for (int i = 0; i < tokenRangeCount; i++) {
            BigInteger start = ringTokens.get(i);
            BigInteger stop = ringTokens.get((i + 1) % tokenRangeCount);

            if (isNotInRange(start) || isNotInRange(stop)) {
                throw new RuntimeException(
                        String.format(
                                "Tokens (%s,%s) not in range of %s", start, stop, partitioner));
            }
            if (start.equals(stop) && tokenRangeCount != 1) {
                throw new RuntimeException(
                        String.format(
                                "Tokens (%s,%s): two nodes have the same token", start, stop));
            }

            BigInteger rangeSize = stop.subtract(start);
            if (rangeSize.compareTo(BigInteger.ZERO) <= 0) {
                // wrap around case
                rangeSize = rangeSize.add(partitioner.ringSize());
            }

            // the below, in essence, does this:
            // splitCount = Maths.ceil((rangeSize / cluster range size) * totalSplitCount)
            BigInteger[] splitCountAndRemainder =
                    rangeSize
                            .multiply(BigInteger.valueOf(totalSplitCount))
                            .divideAndRemainder(partitioner.ringSize());

            int splitCount =
                    splitCountAndRemainder[0].intValue()
                            + (splitCountAndRemainder[1].equals(BigInteger.ZERO) ? 0 : 1);

            LOG.debug("Dividing token range [{},{}) into {} splits", start, stop, splitCount);

            // Make BigInteger list of all the endpoints for the splits, including both start and
            // stop
            List<BigInteger> endpointTokens = new ArrayList<>();
            for (int j = 0; j <= splitCount; j++) {
                BigInteger offset =
                        rangeSize
                                .multiply(BigInteger.valueOf(j))
                                .divide(BigInteger.valueOf(splitCount));
                BigInteger token = start.add(offset);
                if (token.compareTo(partitioner.max()) > 0) {
                    token = token.subtract(partitioner.ringSize());
                }
                // Long.MIN_VALUE is not a valid token and has to be silently incremented.
                // See https://issues.apache.org/jira/browse/CASSANDRA-14684
                endpointTokens.add(
                        token.equals(BigInteger.valueOf(Long.MIN_VALUE))
                                ? token.add(BigInteger.ONE)
                                : token);
            }

            // Append the ringRanges between the endpoints
            for (int j = 0; j < splitCount; j++) {
                ringRanges.add(RingRange.of(endpointTokens.get(j), endpointTokens.get(j + 1)));
                LOG.debug(
                        "Split #{}: [{},{})",
                        j + 1,
                        endpointTokens.get(j),
                        endpointTokens.get(j + 1));
            }
        }

        BigInteger total = BigInteger.ZERO;
        for (RingRange split : ringRanges) {
            BigInteger size = split.span(partitioner.ringSize());
            total = total.add(size);
        }
        if (!total.equals(partitioner.ringSize())) {
            throw new RuntimeException(
                    "Some tokens are missing from the splits. This should not happen.");
        }
        return coalesceRingRanges(getTargetSplitSize(totalSplitCount), ringRanges);
    }

    private boolean isNotInRange(BigInteger token) {
        return token.compareTo(partitioner.min()) < 0 || token.compareTo(partitioner.max()) > 0;
    }

    private List<CassandraSplit> coalesceRingRanges(
            BigInteger targetSplitSize, List<RingRange> ringRanges) {
        List<CassandraSplit> coalescedSplits = new ArrayList<>();
        List<RingRange> tokenRangesForCurrentSplit = new ArrayList<>();
        BigInteger tokenCount = BigInteger.ZERO;

        for (RingRange tokenRange : ringRanges) {
            if (tokenRange.span(partitioner.ringSize()).add(tokenCount).compareTo(targetSplitSize)
                            > 0
                    && !tokenRangesForCurrentSplit.isEmpty()) {
                // enough tokens in that segment
                LOG.debug(
                        "Got enough tokens for one split ({}) : {}",
                        tokenCount,
                        tokenRangesForCurrentSplit);
                coalescedSplits.add(new CassandraSplit(new HashSet<>(tokenRangesForCurrentSplit)));
                tokenRangesForCurrentSplit = new ArrayList<>();
                tokenCount = BigInteger.ZERO;
            }

            tokenCount = tokenCount.add(tokenRange.span(partitioner.ringSize()));
            tokenRangesForCurrentSplit.add(tokenRange);
        }

        if (!tokenRangesForCurrentSplit.isEmpty()) {
            coalescedSplits.add(new CassandraSplit(new HashSet<>(tokenRangesForCurrentSplit)));
        }
        return coalescedSplits;
    }

    private BigInteger getTargetSplitSize(long splitCount) {
        return partitioner.max().subtract(partitioner.min()).divide(BigInteger.valueOf(splitCount));
    }

    /** enum to configure the SplitGenerator based on Apache Cassandra partitioners. */
    public enum CassandraPartitioner {
        MURMUR3PARTITIONER(
                "Murmur3Partitioner",
                BigInteger.valueOf(2).pow(63).negate(),
                BigInteger.valueOf(2).pow(63).subtract(BigInteger.ONE)),
        RANDOMPARTITIONER(
                "RandomPartitioner",
                BigInteger.ZERO,
                BigInteger.valueOf(2).pow(127).subtract(BigInteger.ONE));

        private final BigInteger min;
        private final BigInteger max;
        private final BigInteger ringSize;
        private final String className;

        CassandraPartitioner(String className, BigInteger min, BigInteger max) {
            this.className = className;
            this.min = min;
            this.max = max;
            this.ringSize = max.subtract(min).add(BigInteger.ONE);
        }

        public BigInteger min() {
            return min;
        }

        public BigInteger max() {
            return max;
        }

        public BigInteger ringSize() {
            return ringSize;
        }

        public String className() {
            return className;
        }
    }
}
