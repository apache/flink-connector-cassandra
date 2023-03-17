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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;

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
            objectOutputStream.writeLong(cassandraEnumeratorState.getNumSplitsLeftToGenerate());

            final byte[] increment = cassandraEnumeratorState.getIncrement().toByteArray();
            objectOutputStream.writeInt(increment.length);
            byteArrayOutputStream.write(increment);

            final byte[] startToken = cassandraEnumeratorState.getStartToken().toByteArray();
            objectOutputStream.writeInt(startToken.length);
            byteArrayOutputStream.write(startToken);

            final byte[] maxToken = cassandraEnumeratorState.getMaxToken().toByteArray();
            objectOutputStream.writeInt(maxToken.length);
            byteArrayOutputStream.write(maxToken);

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
            final long numSplitsLeftToGenerate = objectInputStream.readLong();

            final int incrementSize = objectInputStream.readInt();
            final byte[] incrementBytes = new byte[incrementSize];
            if (byteArrayInputStream.read(incrementBytes) == -1) {
                throw new IOException("EOF received while deserializing cassandraEnumeratorState.increment");
            }
            final BigInteger increment = new BigInteger(incrementBytes);

            final int startTokenSize = objectInputStream.readInt();
            final byte[] startTokenBytes = new byte[startTokenSize];
            if (byteArrayInputStream.read(startTokenBytes) == -1) {
                throw new IOException("EOF received while deserializing cassandraEnumeratorState.startToken");
            }
            final BigInteger startToken = new BigInteger(startTokenBytes);

            final int maxTokenSize = objectInputStream.readInt();
            final byte[] maxTokenBytes = new byte[maxTokenSize];
            if (byteArrayInputStream.read(maxTokenBytes) == -1) {
                throw new IOException("EOF received while deserializing cassandraEnumeratorState.maxToken");
            }
            final BigInteger maxToken = new BigInteger(maxTokenBytes);

            return new CassandraEnumeratorState(
                    numSplitsLeftToGenerate, increment, startToken, maxToken);
        }
    }
}
