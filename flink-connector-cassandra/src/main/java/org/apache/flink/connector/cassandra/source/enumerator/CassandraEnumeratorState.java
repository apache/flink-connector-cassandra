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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Objects;
import java.util.Queue;

/** Sate for {@link CassandraSplitEnumerator} to track the splits yet to assign. */
public class CassandraEnumeratorState {
    private final Queue<CassandraSplit> unassignedSplits;

    public CassandraEnumeratorState() {
        this.unassignedSplits = new ArrayDeque<>();
    }

    public CassandraEnumeratorState(Queue<CassandraSplit> unassignedSplits) {
        this.unassignedSplits = unassignedSplits;
    }

    public Queue<CassandraSplit> getUnassignedSplits() {
        return unassignedSplits;
    }

    public void addNewSplits(Collection<CassandraSplit> newSplits) {
        unassignedSplits.addAll(newSplits);
    }

    public CassandraSplit getASplit() {
        return unassignedSplits.poll();
    }

    boolean hasMoreSplits() {
        return unassignedSplits.size() != 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraEnumeratorState that = (CassandraEnumeratorState) o;
        if (this.unassignedSplits.size() != that.unassignedSplits.size()) {
            return false;
        }
        for (CassandraSplit cassandraSplit : unassignedSplits) {
            if (!that.unassignedSplits.contains(cassandraSplit)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(unassignedSplits);
    }
}
