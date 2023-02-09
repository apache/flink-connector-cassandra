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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

/** Serializer for {@link CassandraEnumeratorState}. */
public class CassandraEnumeratorStateSerializer
        implements SimpleVersionedSerializer<CassandraEnumeratorState> {

    public static final CassandraEnumeratorStateSerializer INSTANCE =
            new CassandraEnumeratorStateSerializer();
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));
    public static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(CassandraEnumeratorState cassandraEnumeratorState) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        final Queue<CassandraSplit> unassignedSplits =
                cassandraEnumeratorState.getUnassignedSplits();
        out.writeInt(unassignedSplits.size());
        for (CassandraSplit cassandraSplit : unassignedSplits) {
            final byte[] serializedSplit =
                    CassandraSplitSerializer.INSTANCE.serialize(cassandraSplit);
            out.writeInt(serializedSplit.length);
            out.write(serializedSplit);
        }
        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public CassandraEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        Queue<CassandraSplit> unassignedSplits = new ArrayDeque<>();
        final int unassignedSplitsSize = in.readInt();
        for (int i = 0; i < unassignedSplitsSize; i++) {
            final int splitSize = in.readInt();
            final byte[] splitBytes = new byte[splitSize];
            if (in.read(splitBytes) == -1) {
                throw new IOException("EOF received while deserializing CassandraEnumeratorState");
            }
            final CassandraSplit split =
                    CassandraSplitSerializer.INSTANCE.deserialize(
                            CassandraSplitSerializer.CURRENT_VERSION, splitBytes);
            unassignedSplits.add(split);
        }
        return new CassandraEnumeratorState(unassignedSplits);
    }
}
