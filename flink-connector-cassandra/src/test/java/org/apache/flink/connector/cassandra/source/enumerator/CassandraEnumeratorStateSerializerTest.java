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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CassandraEnumeratorStateSerializer}. */
class CassandraEnumeratorStateSerializerTest {

    @Test
    public void testSerdeRoundtrip() throws Exception {
        final Queue<CassandraSplit> splitsToReassign =
                new ArrayDeque<>(
                        ImmutableList.of(
                                new CassandraSplit(BigInteger.ZERO, BigInteger.TEN),
                                new CassandraSplit(BigInteger.TEN, BigInteger.ZERO)));

        final CassandraEnumeratorState cassandraEnumeratorState =
                new CassandraEnumeratorState(
                        10, BigInteger.ONE, BigInteger.ZERO, BigInteger.TEN, splitsToReassign);

        final byte[] serialized =
                CassandraEnumeratorStateSerializer.INSTANCE.serialize(cassandraEnumeratorState);
        final CassandraEnumeratorState deserialized =
                CassandraEnumeratorStateSerializer.INSTANCE.deserialize(
                        CassandraEnumeratorStateSerializer.CURRENT_VERSION, serialized);
        assertThat(deserialized)
                .isEqualTo(cassandraEnumeratorState)
                .withFailMessage(
                        "CassandraEnumeratorState is not the same as input object after serde roundtrip");
    }
}
