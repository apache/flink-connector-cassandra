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
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
        try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                final ObjectOutputStream objectOutputStream =
                        new ObjectOutputStream(byteArrayOutputStream)) {
            final Queue<CassandraSplit> unassignedSplits =
                    cassandraEnumeratorState.getUnassignedSplits();
            objectOutputStream.writeInt(unassignedSplits.size());
            for (CassandraSplit cassandraSplit : unassignedSplits) {
                final byte[] serializedSplit =
                        CassandraSplitSerializer.INSTANCE.serialize(cassandraSplit);
                objectOutputStream.writeInt(serializedSplit.length);
                objectOutputStream.write(serializedSplit);
            }
            objectOutputStream.flush();
            return byteArrayOutputStream.toByteArray();
        }
    }

    @Override
    public CassandraEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream byteArrayInputStream =
                        new ByteArrayInputStream(serialized);
                final ObjectInputStream objectInputStream =
                        new ObjectInputStream(byteArrayInputStream)) {
            Queue<CassandraSplit> unassignedSplits = new ArrayDeque<>();
            final int unassignedSplitsSize = objectInputStream.readInt();
            for (int i = 0; i < unassignedSplitsSize; i++) {
                final int splitSize = objectInputStream.readInt();
                final byte[] splitBytes = new byte[splitSize];
                if (objectInputStream.read(splitBytes) == -1) {
                    throw new IOException(
                            "EOF received while deserializing CassandraEnumeratorState");
                }
                final CassandraSplit split =
                        CassandraSplitSerializer.INSTANCE.deserialize(
                                CassandraSplitSerializer.CURRENT_VERSION, splitBytes);
                unassignedSplits.add(split);
            }
            return new CassandraEnumeratorState(unassignedSplits);
        }
    }
}
