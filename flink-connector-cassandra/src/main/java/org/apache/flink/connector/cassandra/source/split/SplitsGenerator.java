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
import java.util.List;

/**
 * This class generates {@link CassandraSplit}s based on Cassandra cluster partitioner and Flink
 * source parallelism.
 */
public final class SplitsGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(SplitsGenerator.class);

    private final CassandraPartitioner partitioner;

    public SplitsGenerator(CassandraPartitioner partitioner) {
        this.partitioner = partitioner;
    }

    /**
     * Split Cassandra tokens ring into {@link CassandraSplit}s containing each a range of the ring.
     *
     * @param numSplits requested number of splits
     * @return list containing {@code numSplits} CassandraSplits.
     */
    public List<CassandraSplit> generateSplits(long numSplits) {
        if (numSplits == 1) {
            return Collections.singletonList(
                    new CassandraSplit(partitioner.minToken(), partitioner.maxToken()));
        }
        List<CassandraSplit> splits = new ArrayList<>();
        BigInteger splitSize =
                (partitioner.ringSize()).divide(new BigInteger(String.valueOf(numSplits)));

        BigInteger startToken, endToken = partitioner.minToken();
        for (int splitCount = 1; splitCount <= numSplits; splitCount++) {
            startToken = endToken;
            endToken = startToken.add(splitSize);
            if (splitCount == numSplits) {
                endToken = partitioner.maxToken();
            }
            splits.add(new CassandraSplit(startToken, endToken));
        }
        LOG.debug("Generated {} splits : {}", splits.size(), splits);
        return splits;
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

        private final BigInteger minToken;
        private final BigInteger maxToken;
        private final BigInteger ringSize;
        private final String className;

        CassandraPartitioner(String className, BigInteger minToken, BigInteger maxToken) {
            this.className = className;
            this.minToken = minToken;
            this.maxToken = maxToken;
            this.ringSize = maxToken.subtract(minToken).add(BigInteger.ONE);
        }

        public BigInteger minToken() {
            return minToken;
        }

        public BigInteger maxToken() {
            return maxToken;
        }

        public BigInteger ringSize() {
            return ringSize;
        }

        public String className() {
            return className;
        }
    }
}
