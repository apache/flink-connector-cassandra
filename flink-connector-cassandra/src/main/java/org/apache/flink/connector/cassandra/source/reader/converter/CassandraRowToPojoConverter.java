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

package org.apache.flink.connector.cassandra.source.reader.converter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.cassandra.mapper.PojoMapper;
import org.apache.flink.connector.cassandra.source.reader.CassandraRow;

import com.datastax.oss.driver.api.core.cql.Row;

import static java.util.Objects.requireNonNull;

/**
 * Cassandra row converter using {@link PojoMapper} to convert Cassandra's {@link Row} to POJO.
 *
 * @param <OUT> The POJO type
 */
@Internal
public class CassandraRowToPojoConverter<OUT> implements CassandraRowToTypeConverter<OUT> {

    private static final long serialVersionUID = 1L;

    private final Class<OUT> pojoClass;
    private transient PojoMapper<OUT> mapper;

    public CassandraRowToPojoConverter(Class<OUT> pojoClass) {
        this.pojoClass = requireNonNull(pojoClass, "POJO class cannot be null");
        // fail fast if the class cannot be mapped
        PojoMapper.create(pojoClass);
    }

    @Override
    public OUT convert(CassandraRow cassandraRow) {
        if (mapper == null) {
            mapper = PojoMapper.create(pojoClass);
        }
        return mapper.map(cassandraRow.getRow());
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return TypeInformation.of(pojoClass);
    }
}
