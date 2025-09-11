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

package org.apache.flink.connector.cassandra.sink.config;

import org.apache.flink.annotation.PublicEvolving;

/** Enum of the supported format types. */
@PublicEvolving
public enum RecordFormatType {

    /** Flink Row type. Fields are extracted using {@code row.getField(index)}. */
    ROW("row"),

    /**
     * Flink Tuple types (Tuple1, Tuple2, etc.). Fields are extracted using {@code
     * tuple.getField(index)}.
     */
    TUPLE("tuple"),

    /**
     * Scala Product types (case classes). Fields are extracted using {@code
     * product.productElement(index)}.
     */
    SCALA_PRODUCT("scala_product"),

    /**
     * Flink RowData type (used internally by Table/SQL API). Fields are extracted using
     * RowData.FieldGetter instances.
     */
    ROWDATA("rowdata"),

    /**
     * POJO types with DataStax Mapper annotations. Serialization is handled by the DataStax Mapper.
     */
    POJO("pojo");

    private final String name;

    RecordFormatType(String name) {
        this.name = name;
    }

    /**
     * Gets the string representation of this record format type.
     *
     * @return the format type name
     */
    public String getName() {
        return name;
    }
}
