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

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.math.BigInteger;

/** Serializer for {@link CassandraSplit}. */
public class CassandraSplitSerializer implements SimpleVersionedSerializer<CassandraSplit> {

    public static final CassandraSplitSerializer INSTANCE = new CassandraSplitSerializer();
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    public static final int CURRENT_VERSION = 0;

    private CassandraSplitSerializer() {}

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(CassandraSplit cassandraSplit) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        final byte[] ringRangeStart = cassandraSplit.getRingRangeStart().toByteArray();
        out.writeInt(ringRangeStart.length);
        out.write(ringRangeStart);
        final byte[] ringRangeEnd = cassandraSplit.getRingRangeEnd().toByteArray();
        out.writeInt(ringRangeEnd.length);
        out.write(ringRangeEnd);
        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public CassandraSplit deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        final int ringRangeStartSize = in.readInt();
        final byte[] ringRangeStartBytes = new byte[ringRangeStartSize];
        if (in.read(ringRangeStartBytes) == -1) {
            throw new IOException("EOF received while deserializing CassandraSplit ringRangeStart");
        }
        final BigInteger ringRangeStart = new BigInteger(ringRangeStartBytes);
        final int ringRangeEndSize = in.readInt();
        final byte[] ringRangeEndBytes = new byte[ringRangeEndSize];
        if (in.read(ringRangeEndBytes) == -1) {
            throw new IOException("EOF received while deserializing CassandraSplit ringRangeEnd");
        }
        final BigInteger ringRangeEnd = new BigInteger(ringRangeEndBytes);
        return new CassandraSplit(ringRangeStart, ringRangeEnd);
    }
}
