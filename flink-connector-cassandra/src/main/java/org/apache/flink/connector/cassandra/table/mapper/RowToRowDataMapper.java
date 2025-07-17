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

package org.apache.flink.connector.cassandra.table.mapper;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.datastax.driver.core.Row;

import java.io.Serializable;

/**
 * Mapper for converting Cassandra {@link Row} to Flink's {@link RowData} with proper type handling.
 */
@Internal
public class RowToRowDataMapper implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CassandraFieldMapper[] fieldMappers;
    private final String[] fieldNames;
    private final int fieldCount;

    public RowToRowDataMapper(RowType rowType) {
        this.fieldCount = rowType.getFieldCount();
        this.fieldNames = rowType.getFieldNames().toArray(new String[0]);
        this.fieldMappers = new CassandraFieldMapper[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            this.fieldMappers[i] =
                    CassandraFieldMapperFactory.createFieldMapper(rowType.getTypeAt(i));
        }
    }

    public RowToRowDataMapper(RowType rowType, CassandraFieldMapper[] fieldMappers) {
        this.fieldCount = rowType.getFieldCount();
        this.fieldNames = rowType.getFieldNames().toArray(new String[0]);
        this.fieldMappers = fieldMappers;

        if (fieldMappers.length != fieldCount) {
            throw new IllegalArgumentException(
                    String.format(
                            "Field mappers count (%d) does not match row type field count (%d)",
                            fieldMappers.length, fieldCount));
        }
    }

    public RowData convert(Row row) {
        if (row == null) {
            return null;
        }

        GenericRowData rowData = new GenericRowData(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            rowData.setField(i, fieldMappers[i].extractFromRow(row, fieldNames[i]));
        }
        return rowData;
    }
}
