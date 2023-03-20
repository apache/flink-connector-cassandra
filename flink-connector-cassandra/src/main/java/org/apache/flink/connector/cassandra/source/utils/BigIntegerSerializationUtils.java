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

package org.apache.flink.connector.cassandra.source.utils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;

/** Utils for BigInteger reading and writing in serde context. */
public class BigIntegerSerializationUtils {
    public static void write(BigInteger bigInteger, ObjectOutputStream objectOutputStream)
            throws IOException {
        final byte[] bigIntegerBytes = bigInteger.toByteArray();
        objectOutputStream.writeInt(bigIntegerBytes.length);
        objectOutputStream.write(bigIntegerBytes);
    }

    public static BigInteger read(ObjectInputStream objectInputStream) throws IOException {
        final int bigIntegerSize = objectInputStream.readInt();
        final byte[] bigIntegerBytes = new byte[bigIntegerSize];
        if (objectInputStream.read(bigIntegerBytes) == -1) {
            throw new IOException(
                    "EOF received while deserializing cassandraEnumeratorState.increment");
        }
        return new BigInteger(bigIntegerBytes);
    }
}
