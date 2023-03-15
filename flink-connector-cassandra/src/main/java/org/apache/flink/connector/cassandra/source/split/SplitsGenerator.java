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

import org.apache.flink.annotation.VisibleForTesting;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * This class generates {@link CassandraSplit}s based on Cassandra cluster partitioner and cluster
 * statistics. It estimates the total size of the table using Cassandra system table
 * system.size_estimates. But there is no way to estimate the size of the data with the optional SQL
 * filters without reading the data. So the splits can be smaller than {@param maxSplitMemorySize}
 * when the query is executed.
 */
public final class SplitsGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(SplitsGenerator.class);
    private static final int ACCEPTABLE_NB_SPLIT_PARALLELISM_RATIO = 10;

    private final CassandraPartitioner partitioner;
    private final Session session;
    private final String keyspace;
    private final String table;
    private final int parallelism;
    @Nullable private final Long maxSplitMemorySize;

    public SplitsGenerator(
            CassandraPartitioner partitioner,
            Session session,
            String keyspace,
            String table,
            int parallelism,
            Long maxSplitMemorySize) {
        this.partitioner = partitioner;
        this.session = session;
        this.keyspace = keyspace;
        this.table = table;
        this.parallelism = parallelism;
        this.maxSplitMemorySize = maxSplitMemorySize;
    }

    /**
     * Split Cassandra tokens ring into {@link CassandraSplit}s containing each a range of the
     * Cassandra ring of {@param maxSplitMemorySize}. If {@param maxSplitMemorySize} is not defined,
     * or is too high or too low compared to the task parallelism, then it generates as many {@link
     * CassandraSplit}s as the task parallelism.
     *
     * @return list containing {@code numSplits} CassandraSplits.
     */
    public List<CassandraSplit> generateSplits() {
        long numSplits;
        if (maxSplitMemorySize != null) {
            final long estimateTableSize = estimateTableSize();
            LOG.debug("Estimated table size for table {} is {} bytes", table, estimateTableSize);
            numSplits = estimateTableSize / maxSplitMemorySize;
            if (numSplits == 0 // estimateTableSize can be null in some cases (see javadoc)
                    || numSplits < parallelism / ACCEPTABLE_NB_SPLIT_PARALLELISM_RATIO // too low
                    || numSplits
                            > (long) parallelism
                                    * ACCEPTABLE_NB_SPLIT_PARALLELISM_RATIO) { // too high
                LOG.info(
                        "maxSplitMemorySize set value leads to {} splits with a task parallelism of {}. Creating as many splits as parallelism",
                        numSplits,
                        parallelism);
                numSplits = parallelism;
            }
        } else { // not defined
            LOG.info(
                    "maxSplitMemorySize not set. Creating as many splits as parallelism ({})",
                    parallelism);
            numSplits = parallelism;
        }

        List<CassandraSplit> splits = new ArrayList<>();
        BigInteger increment =
                (partitioner.ringSize).divide(new BigInteger(String.valueOf(numSplits)));

        BigInteger startToken = partitioner.minToken;
        for (int splitCount = 1; splitCount <= numSplits; splitCount++) {
            BigInteger endToken = startToken.add(increment);
            if (splitCount == numSplits) {
                endToken = partitioner.maxToken;
            }
            splits.add(new CassandraSplit(startToken, endToken));
            startToken = endToken;
        }
        LOG.debug("Generated {} splits : {}", splits.size(), splits);
        return splits;
    }

    /**
     * Estimates the size of the table in bytes. Cassandra size estimates can be 0 if the data was
     * just inserted and the amount of data in the table was small. This is very common situation
     * during tests.
     */
    @VisibleForTesting
    public long estimateTableSize() {
        List<TokenRange> tokenRanges = getTokenRangesOfTable();
        long size = 0L;
        for (TokenRange tokenRange : tokenRanges) {
            size += tokenRange.meanPartitionSize * tokenRange.partitionCount;
        }
        return Math.round(size / getRingFraction(tokenRanges));
    }

    /**
     * The values that we get from system.size_estimates are for one node. We need to extrapolate to
     * the whole cluster. This method estimates the percentage, the node represents in the cluster.
     *
     * @param tokenRanges The list of {@link TokenRange} to estimate
     * @return The percentage the node represent in the whole cluster
     */
    private float getRingFraction(List<TokenRange> tokenRanges) {
        BigInteger addressedTokens = BigInteger.ZERO;
        for (TokenRange tokenRange : tokenRanges) {
            addressedTokens =
                    addressedTokens.add(distance(tokenRange.rangeStart, tokenRange.rangeEnd));
        }
        // it is < 1 because it is a percentage
        return Float.valueOf(addressedTokens.divide(partitioner.ringSize).toString());
    }

    /** Gets the list of token ranges that the table occupies on a given Cassandra node. */
    private List<TokenRange> getTokenRangesOfTable() {
        ResultSet resultSet =
                session.execute(
                        "SELECT range_start, range_end, partitions_count, mean_partition_size FROM "
                                + "system.size_estimates WHERE keyspace_name = ? AND table_name = ?",
                        keyspace,
                        table);

        ArrayList<TokenRange> tokenRanges = new ArrayList<>();
        for (Row row : resultSet) {
            TokenRange tokenRange =
                    new TokenRange(
                            row.getLong("partitions_count"),
                            row.getLong("mean_partition_size"),
                            row.getString("range_start"),
                            row.getString("range_end"));
            tokenRanges.add(tokenRange);
        }
        // The table may not contain the estimates yet
        // or have partitions_count and mean_partition_size fields = 0
        // if the data was just inserted and the amount of data in the table was small.
        // This is very common situation during tests,
        // when we insert a few rows and immediately query them.
        // However, for tiny data sets the lack of size estimates is not a problem at all,
        // because we don't want to split tiny data anyways.
        // Therefore, we're not issuing a warning if the result set was empty
        // or mean_partition_size and partitions_count = 0.
        return tokenRanges;
    }

    /**
     * Measure distance between two tokens.
     *
     * @param token1 The measure is symmetrical so token1 and token2 can be exchanged
     * @param token2 The measure is symmetrical so token1 and token2 can be exchanged
     * @return Number of tokens that separate token1 and token2
     */
    private BigInteger distance(BigInteger token1, BigInteger token2) {
        // token2 > token1
        if (token2.compareTo(token1) == 1) {
            return token2.subtract(token1);
        } else {
            return token2.subtract(token1).add(partitioner.ringSize);
        }
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

        public String getClassName() {
            return className;
        }
    }

    private static class TokenRange {
        private final long partitionCount;
        private final long meanPartitionSize;
        private final BigInteger rangeStart;
        private final BigInteger rangeEnd;

        private TokenRange(
                long partitionCount, long meanPartitionSize, String rangeStart, String rangeEnd) {
            this.partitionCount = partitionCount;
            this.meanPartitionSize = meanPartitionSize;
            this.rangeStart = new BigInteger(rangeStart);
            this.rangeEnd = new BigInteger(rangeEnd);
        }
    }
}
