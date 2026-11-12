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

import org.apache.flink.annotation.Internal;

import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Date;

/**
 * Codec mapping CQL {@code timestamp} to {@link Date}. The driver only maps {@code timestamp} to
 * {@link Instant} by default, this codec keeps {@link Date} values usable in the connector.
 */
@Internal
public final class DateTimestampCodec extends MappingCodec<Instant, Date> {

    public static final DateTimestampCodec INSTANCE = new DateTimestampCodec();

    private DateTimestampCodec() {
        super(TypeCodecs.TIMESTAMP, GenericType.of(Date.class));
    }

    @Nullable
    @Override
    protected Date innerToOuter(@Nullable Instant value) {
        return value == null ? null : Date.from(value);
    }

    @Nullable
    @Override
    protected Instant outerToInner(@Nullable Date value) {
        return value == null ? null : Instant.ofEpochMilli(value.getTime());
    }
}
