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

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.cassandra.source.split.SplitsGenerator.CassandraPartitioner.MURMUR3PARTITIONER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SplitsGenerator}. */
public final class SplitsGeneratorTest {

    @Test
    public void testGenerateSplits() {
        List<BigInteger> tokens =
                Stream.of(
                                "0",
                                "1",
                                "567137278201564105",
                                "567137278201564106",
                                "897137278201564106",
                                "897137278201564107")
                        .map(BigInteger::new)
                        .collect(Collectors.toList());

        SplitsGenerator generator = new SplitsGenerator(MURMUR3PARTITIONER);
        List<CassandraSplit> splits = generator.generateSplits(10, tokens);

        assertThat(splits.size()).isEqualTo(11);
        assertThat(splits.get(0).splitId())
                .isEqualTo(
                        "[(567137278201564106,897137278201564106], (0,1], (567137278201564105,567137278201564106], (1,567137278201564105], (897137278201564106,897137278201564107]]");
        assertThat(splits.get(1).splitId()).isEqualTo("[(897137278201564107,2652097957752362857]]");
        assertThat(splits.get(5).splitId())
                .isEqualTo("[(7916979996404759110,-8774803397753993755]]");

        tokens =
                Stream.of(
                                "5",
                                "6",
                                "567137278201564105",
                                "567137278201564106",
                                "897137278201564106",
                                "897137278201564107")
                        .map(BigInteger::new)
                        .collect(Collectors.toList());

        splits = generator.generateSplits(10, tokens);

        assertThat(splits.size()).isEqualTo(11);
        assertThat(splits.get(0).splitId())
                .isEqualTo(
                        "[(567137278201564106,897137278201564106], (5,6], (567137278201564105,567137278201564106], (6,567137278201564105], (897137278201564106,897137278201564107]]");
        assertThat(splits.get(5).splitId())
                .isEqualTo("[(7916979996404759112,-8774803397753993752]]");
        assertThat(splits.get(10).splitId()).isEqualTo("[(-1754960679550798747,5]]");
    }

    @Test
    public void testZeroSizeRange() {
        List<String> tokenStrings =
                Arrays.asList(
                        "0",
                        "1",
                        "567137278201564105",
                        "567137278201564105",
                        "897137278201564106",
                        "897137278201564107");

        List<BigInteger> tokens =
                tokenStrings.stream().map(BigInteger::new).collect(Collectors.toList());

        SplitsGenerator generator = new SplitsGenerator(MURMUR3PARTITIONER);
        assertThatThrownBy(() -> generator.generateSplits(10, tokens))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testRotatedRing() {
        List<String> tokenStrings =
                Arrays.asList(
                        "567137278201564106",
                        "897137278201564106",
                        "897137278201564107",
                        "5",
                        "6",
                        "567137278201564105");

        List<BigInteger> tokens =
                tokenStrings.stream().map(BigInteger::new).collect(Collectors.toList());

        SplitsGenerator generator = new SplitsGenerator(MURMUR3PARTITIONER);
        List<CassandraSplit> splits = generator.generateSplits(5, tokens);
        assertThat(splits.size()).isEqualTo(7);

        assertThat(splits.get(1))
                .isEqualTo(
                        new CassandraSplit(
                                ImmutableSet.of(
                                        RingRange.of(
                                                new BigInteger("897137278201564107"),
                                                new BigInteger("4407058637303161609")))));

        assertThat(splits.get(2))
                .isEqualTo(
                        new CassandraSplit(
                                ImmutableSet.of(
                                        RingRange.of(
                                                new BigInteger("4407058637303161609"),
                                                new BigInteger("7916979996404759112")))));

        assertThat(splits.get(3))
                .isEqualTo(
                        new CassandraSplit(
                                ImmutableSet.of(
                                        RingRange.of(
                                                new BigInteger("7916979996404759112"),
                                                new BigInteger("-7019842718203195001")))));
    }

    @Test
    public void testDisorderedRing() {
        List<String> tokenStrings =
                Arrays.asList(
                        "567137278201564105",
                        "567137278201564106",
                        "0",
                        "1",
                        "897137278201564106",
                        "897137278201564107");

        List<BigInteger> tokens =
                tokenStrings.stream().map(BigInteger::new).collect(Collectors.toList());

        SplitsGenerator generator = new SplitsGenerator(MURMUR3PARTITIONER);
        // Will throw an exception when concluding that the repair segments don't add up.
        // This is because the tokens were supplied out of order.
        assertThatThrownBy(() -> generator.generateSplits(10, tokens))
                .isInstanceOf(RuntimeException.class);
    }
}
