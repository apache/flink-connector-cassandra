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

package org.apache.flink.connector.cassandra.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DateTimestampCodec}. */
class DateTimestampCodecTest {

    private static final long EPOCH_MILLIS = 1700000000123L;

    @Test
    void testTypes() {
        assertThat(DateTimestampCodec.INSTANCE.getCqlType()).isEqualTo(DataTypes.TIMESTAMP);
        assertThat(DateTimestampCodec.INSTANCE.getJavaType()).isEqualTo(GenericType.of(Date.class));
        assertThat(DateTimestampCodec.INSTANCE.accepts(Date.class)).isTrue();
        assertThat(DateTimestampCodec.INSTANCE.accepts(Instant.class)).isFalse();
    }

    @Test
    void testEncodeDecodeRoundTrip() {
        final Date date = new Date(EPOCH_MILLIS);

        final ByteBuffer encoded =
                DateTimestampCodec.INSTANCE.encode(date, ProtocolVersion.DEFAULT);

        // the binary format is the one of the timestamp codec of the driver
        assertThat(encoded)
                .isEqualTo(
                        TypeCodecs.TIMESTAMP.encode(
                                Instant.ofEpochMilli(EPOCH_MILLIS), ProtocolVersion.DEFAULT));
        assertThat(DateTimestampCodec.INSTANCE.decode(encoded, ProtocolVersion.DEFAULT))
                .isEqualTo(date);
    }

    @Test
    void testNull() {
        assertThat(DateTimestampCodec.INSTANCE.encode(null, ProtocolVersion.DEFAULT)).isNull();
        assertThat(DateTimestampCodec.INSTANCE.decode(null, ProtocolVersion.DEFAULT)).isNull();
    }

    @Test
    void testSubclassOfDateIsAccepted() {
        final Timestamp timestamp = new Timestamp(EPOCH_MILLIS);

        assertThat(DateTimestampCodec.INSTANCE.accepts(timestamp)).isTrue();
        assertThat(
                        DateTimestampCodec.INSTANCE.decode(
                                DateTimestampCodec.INSTANCE.encode(
                                        timestamp, ProtocolVersion.DEFAULT),
                                ProtocolVersion.DEFAULT))
                .isEqualTo(new Date(EPOCH_MILLIS));
    }
}
