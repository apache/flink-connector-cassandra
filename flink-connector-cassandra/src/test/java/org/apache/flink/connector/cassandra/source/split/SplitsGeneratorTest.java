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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.connector.cassandra.source.split.SplitsGenerator.CassandraPartitioner.MURMUR3PARTITIONER;
import static org.apache.flink.connector.cassandra.source.split.SplitsGenerator.CassandraPartitioner.RANDOMPARTITIONER;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SplitsGenerator}. */
class SplitsGeneratorTest {

    @Test
    public void testGenerateSplitsMurMur3Partitioner() {
        SplitsGenerator generator = new SplitsGenerator(MURMUR3PARTITIONER);
        final int numSplits = 2;
        List<CassandraSplit> splits = generator.generateSplits(numSplits);

        assertThat(splits.size()).isEqualTo(numSplits);
        assertThat(splits.get(0).splitId()).isEqualTo("(-9223372036854775808,0)");
        assertThat(splits.get(1).splitId()).isEqualTo("(0,9223372036854775807)");
    }

    @Test
    public void testGenerateSplitsRandomPartitioner() {
        SplitsGenerator generator = new SplitsGenerator(RANDOMPARTITIONER);
        final int numSplits = 2;
        List<CassandraSplit> splits = generator.generateSplits(numSplits);

        assertThat(splits.size()).isEqualTo(numSplits);
        assertThat(splits.get(0).splitId()).isEqualTo("(0,85070591730234615865843651857942052864)");
        assertThat(splits.get(1).splitId())
                .isEqualTo(
                        "(85070591730234615865843651857942052864,170141183460469231731687303715884105727)");
    }
}
