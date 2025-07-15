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
import org.apache.flink.connector.cassandra.source.reader.CassandraRow;
import org.apache.flink.connector.cassandra.table.mapper.CassandraFieldMapper;
import org.apache.flink.connector.cassandra.table.mapper.RowToRowDataMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Cassandra row converter using to convert Cassandra Row to {@link
 * org.apache.flink.table.data.RowData} .
 */
@Internal
public class CassandraRowToRowDataConverter implements CassandraRowToTypeConverter<RowData> {

    private static final long serialVersionUID = 1L;

    private final RowToRowDataMapper mapper;

    public CassandraRowToRowDataConverter(RowType rowType) {
        this.mapper = new RowToRowDataMapper(rowType);
    }

    public CassandraRowToRowDataConverter(RowType rowType, CassandraFieldMapper[] fieldMappers) {
        this.mapper = new RowToRowDataMapper(rowType, fieldMappers);
    }

    @Override
    public RowData convert(CassandraRow cassandraRow) {
        return mapper.convert(cassandraRow.getRow());
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(RowData.class);
    }
}
