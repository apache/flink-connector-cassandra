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
import org.apache.flink.connector.cassandra.source.split.CassandraSplitSerializer;
import org.apache.flink.connector.cassandra.source.utils.BigIntegerSerializationUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Queue;

/** Serializer for {@link CassandraEnumeratorState}. */
public class CassandraEnumeratorStateSerializer
        implements SimpleVersionedSerializer<CassandraEnumeratorState> {

    public static final CassandraEnumeratorStateSerializer INSTANCE =
            new CassandraEnumeratorStateSerializer();
    public static final int CURRENT_VERSION = 0;

    private CassandraEnumeratorStateSerializer() {}

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(CassandraEnumeratorState cassandraEnumeratorState) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (final ObjectOutputStream objectOutputStream =
                new ObjectOutputStream(byteArrayOutputStream)) {
            final Queue<CassandraSplit> splitsToReassign =
                    cassandraEnumeratorState.getSplitsToReassign();
            objectOutputStream.writeInt(splitsToReassign.size());
            for (CassandraSplit cassandraSplit : splitsToReassign) {
                final byte[] serializedSplit =
                        CassandraSplitSerializer.INSTANCE.serialize(cassandraSplit);
                objectOutputStream.writeInt(serializedSplit.length);
                objectOutputStream.write(serializedSplit);
            }

            objectOutputStream.writeLong(cassandraEnumeratorState.getNumSplitsLeftToGenerate());
            BigIntegerSerializationUtils.write(
                    cassandraEnumeratorState.getIncrement(), objectOutputStream);
            BigIntegerSerializationUtils.write(
                    cassandraEnumeratorState.getStartToken(), objectOutputStream);
            BigIntegerSerializationUtils.write(
                    cassandraEnumeratorState.getMaxToken(), objectOutputStream);
        }
        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public CassandraEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream byteArrayInputStream =
                        new ByteArrayInputStream(serialized);
                final ObjectInputStream objectInputStream =
                        new ObjectInputStream(byteArrayInputStream)) {
            final Queue<CassandraSplit> splitsToReassign = new ArrayDeque<>();
            final int splitsToReassignSize = objectInputStream.readInt();
            for (int i = 0; i < splitsToReassignSize; i++) {
                final int splitSize = objectInputStream.readInt();
                final byte[] splitBytes = new byte[splitSize];
                objectInputStream.readFully(splitBytes);
                final CassandraSplit split =
                        CassandraSplitSerializer.INSTANCE.deserialize(
                                CassandraSplitSerializer.CURRENT_VERSION, splitBytes);
                splitsToReassign.add(split);
            }

            final long numSplitsLeftToGenerate = objectInputStream.readLong();
            final BigInteger increment = BigIntegerSerializationUtils.read(objectInputStream);
            final BigInteger startToken = BigIntegerSerializationUtils.read(objectInputStream);
            final BigInteger maxToken = BigIntegerSerializationUtils.read(objectInputStream);

            return new CassandraEnumeratorState(
                    numSplitsLeftToGenerate, increment, startToken, maxToken, splitsToReassign);
        }
    }
}
