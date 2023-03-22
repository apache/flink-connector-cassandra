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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

/** Utils for BigInteger reading and writing in serde context. */
public class BigIntegerSerializationUtils {
    public static void write(BigInteger bigInteger, DataOutput output) throws IOException {
        final byte[] bigIntegerBytes = bigInteger.toByteArray();
        output.writeInt(bigIntegerBytes.length);
        output.write(bigIntegerBytes);
    }

    public static BigInteger read(DataInput input) throws IOException {
        final int bigIntegerSize = input.readInt();
        final byte[] bigIntegerBytes = new byte[bigIntegerSize];
        input.readFully(bigIntegerBytes);
        return new BigInteger(bigIntegerBytes);
    }
}
