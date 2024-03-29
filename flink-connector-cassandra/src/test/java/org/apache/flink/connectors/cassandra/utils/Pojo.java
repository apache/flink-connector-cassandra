/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.cassandra.utils;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;
import java.util.Objects;

/** Test Pojo with DataStax annotations used. */
@Table(keyspace = "flink", name = "batches")
public class Pojo implements Serializable {

    private static final long serialVersionUID = 1038054554690916991L;

    @Column(name = "id")
    private String id;

    @Column(name = "counter")
    private int counter;

    @Column(name = "batch_id")
    private int batchID;

    // required for deserialization
    public Pojo() {}

    public Pojo(String id, int counter, int batchID) {
        this.id = id;
        this.counter = counter;
        this.batchID = batchID;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }

    public int getBatchID() {
        return batchID;
    }

    public void setBatchID(int batchId) {
        this.batchID = batchId;
    }

    @Override
    public String toString() {
        return String.format(
                "{\"id\":\"%s\", \"counter\":%d, \"batchID\":%d}", id, counter, batchID);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pojo pojo = (Pojo) o;
        return counter == pojo.counter && batchID == pojo.batchID && id.equals(pojo.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, counter, batchID);
    }
}
