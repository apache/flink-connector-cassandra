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

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable {@link SourceSplit} for Cassandra source. A Cassandra split is a set of {@link
 * RingRange}s (a range between 2 tokens). Tokens are spread across the Cassandra cluster with each
 * node managing a share of the token ring. Each split can contain several token ranges in order to
 * reduce the overhead on Cassandra vnodes.
 */
public class CassandraSplit implements SourceSplit, Serializable {

    private final Set<RingRange> ringRanges;

    public CassandraSplit(Set<RingRange> ringRanges) {
        this.ringRanges = ringRanges;
    }

    @Override
    public String splitId() {
        return ringRanges.toString();
    }

    public CassandraSplitState toSplitState() {
        return new CassandraSplitState(new HashSet<>(ringRanges), splitId());
    }

    public void serialize(ObjectOutputStream objectOutputStream) throws IOException {
        objectOutputStream.writeInt(ringRanges.size());
        for (RingRange ringRange : ringRanges) {
            objectOutputStream.writeObject(ringRange.getStart());
            objectOutputStream.writeObject(ringRange.getEnd());
        }
    }

    public static CassandraSplit deserialize(ObjectInputStream objectInputStream)
            throws IOException {
        try {
            final int nbRingRanges = objectInputStream.readInt();
            Set<RingRange> ringRanges = new HashSet<>(nbRingRanges);
            for (int i = 0; i < nbRingRanges; i++) {
                final BigInteger start = (BigInteger) objectInputStream.readObject();
                final BigInteger end = (BigInteger) objectInputStream.readObject();
                ringRanges.add(RingRange.of(start, end));
            }
            return new CassandraSplit(ringRanges);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraSplit that = (CassandraSplit) o;
        return ringRanges.equals(that.ringRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ringRanges);
    }
}
