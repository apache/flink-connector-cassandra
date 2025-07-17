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
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;

/** Factory for creating appropriate field mappers based on Flink logical types. */
@Internal
public final class CassandraFieldMapperFactory {

    private CassandraFieldMapperFactory() {}

    /**
     * Creates a field mapper for the given logical type. Different mappers for different {@link
     * com.datastax.driver.core.DataType}
     *
     * @param logicalType the Flink logical type
     * @return appropriate field mapper for the type
     */
    public static CassandraFieldMapper createFieldMapper(LogicalType logicalType) {
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();

        switch (typeRoot) {
                // Supported primitive types
            case BOOLEAN:
                return new PrimitiveFieldMappers.BooleanMapper();
            case TINYINT:
                return new PrimitiveFieldMappers.ByteMapper();
            case SMALLINT:
                return new PrimitiveFieldMappers.ShortMapper();
            case INTEGER:
                return new PrimitiveFieldMappers.IntegerMapper();
            case BIGINT:
                return new PrimitiveFieldMappers.LongMapper();
            case FLOAT:
                return new PrimitiveFieldMappers.FloatMapper();
            case DOUBLE:
                return new PrimitiveFieldMappers.DoubleMapper();
            case VARCHAR:
            case CHAR:
                return new PrimitiveFieldMappers.StringMapper();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                return new PrimitiveFieldMappers.DynamicDecimalMapper(decimalType);
            case DATE:
                return new PrimitiveFieldMappers.DateMapper();
            case TIME_WITHOUT_TIME_ZONE:
                return new PrimitiveFieldMappers.TimeMapper();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new PrimitiveFieldMappers.TimestampMapper();
            case BINARY:
            case VARBINARY:
                return new PrimitiveFieldMappers.BinaryMapper();

                // Supported collection types
            case ARRAY:
                ArrayType arrayType = (ArrayType) logicalType;
                CassandraFieldMapper elementMapper = createFieldMapper(arrayType.getElementType());
                return new CollectionFieldMappers.ArrayMapper(elementMapper);
            case MAP:
                MapType mapType = (MapType) logicalType;
                CassandraFieldMapper keyMapper = createFieldMapper(mapType.getKeyType());
                CassandraFieldMapper valueMapper = createFieldMapper(mapType.getValueType());
                return new CollectionFieldMappers.MapMapper(keyMapper, valueMapper);
            case MULTISET:
                MultisetType multisetType = (MultisetType) logicalType;
                CassandraFieldMapper setElementMapper =
                        createFieldMapper(multisetType.getElementType());
                return new CollectionFieldMappers.SetMapper(setElementMapper);
            case ROW:
                RowType rowType = (RowType) logicalType;
                CassandraFieldMapper[] fieldMappers =
                        new CassandraFieldMapper[rowType.getFieldCount()];
                String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    fieldMappers[i] = createFieldMapper(rowType.getTypeAt(i));
                }
                return new CollectionFieldMappers.RowMapper(fieldMappers, fieldNames);

                // Timezone-aware types - not supported by Cassandra
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException(
                        "TIMESTAMP_WITH_TIME_ZONE is not supported. "
                                + "Cassandra timestamps are timezone-naive (stored as UTC). "
                                + "Use TIMESTAMP_WITHOUT_TIME_ZONE and handle timezone conversion in your application.");
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                throw new UnsupportedOperationException(
                        "TIMESTAMP_WITH_LOCAL_TIME_ZONE is not supported. "
                                + "Cassandra timestamps are timezone-naive (stored as UTC). "
                                + "Use TIMESTAMP_WITHOUT_TIME_ZONE and handle timezone conversion in your application.");

                // Interval types - not supported by Cassandra
            case INTERVAL_YEAR_MONTH:
                throw new UnsupportedOperationException(
                        "INTERVAL_YEAR_MONTH is not supported. "
                                + "Cassandra does not have native interval types. "
                                + "Consider using BIGINT to store interval duration in a specific unit.");
            case INTERVAL_DAY_TIME:
                throw new UnsupportedOperationException(
                        "INTERVAL_DAY_TIME is not supported. "
                                + "Cassandra does not have native interval types. "
                                + "Consider using BIGINT to store interval duration in a specific unit.");

                // User-defined types - complex mapping required
            case DISTINCT_TYPE:
                throw new UnsupportedOperationException(
                        "DISTINCT_TYPE is not supported. "
                                + "Cassandra does not have distinct types. "
                                + "Use the underlying source type directly.");
            case STRUCTURED_TYPE:
                throw new UnsupportedOperationException(
                        "STRUCTURED_TYPE is not supported. "
                                + "While Cassandra has UDTs, StructuredType requires complex catalog integration. "
                                + "Use ROW type to map Cassandra UDTs.");

                // Extension types - not applicable
            case NULL:
                throw new UnsupportedOperationException(
                        "NULL type is not supported. "
                                + "NULL is a value state, not a data type. "
                                + "Use nullable versions of other supported types.");
            case RAW:
                throw new UnsupportedOperationException(
                        "RAW type is not supported. "
                                + "RAW is for bridging to JVM types that don't have a logical representation. "
                                + "Use BINARY/VARBINARY for binary data or define a proper logical type.");
            case SYMBOL:
                throw new UnsupportedOperationException(
                        "SYMBOL type is not supported. "
                                + "SYMBOL is an internal Flink type for planner symbols. "
                                + "This should not appear in user schemas.");
            case UNRESOLVED:
                throw new UnsupportedOperationException(
                        "UNRESOLVED type is not supported. "
                                + "UNRESOLVED indicates a type that could not be resolved during planning. "
                                + "Check your table schema definition.");

            default:
                throw new UnsupportedOperationException(
                        "Unknown logical type: "
                                + logicalType
                                + ". This may be a new type not yet handled.");
        }
    }
}
