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

package org.apache.flink.connector.cassandra.sink.planner.resolver;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * Resolves which Cassandra table to write a record to.
 *
 * <p>This abstraction allows for dynamic table selection based on record content, such as
 * date-based table rotation, sharding strategies, or tenant-based routing.
 *
 * <p><strong>Example Use Cases:</strong>
 *
 * <pre>{@code
 * // Example 1: Date-based table partitioning
 * TableResolver<Row> dateBasedResolver = record -> {
 *     Timestamp timestamp = (Timestamp) record.getField(0);
 *     String dateStr = new SimpleDateFormat("yyyyMM").format(timestamp);
 *     return new TableRef("analytics", "events_" + dateStr);
 * };
 *
 * // Example 2: Tenant-based table routing
 * TableResolver<Row> tenantResolver = record -> {
 *     String tenantId = (String) record.getField(0);
 *     return new TableRef("multi_tenant", "tenant_" + tenantId + "_data");
 * };
 *
 * // Example 3: Record type-based routing
 * TableResolver<Row> typeBasedResolver = record -> {
 *     // Assuming type field is at index 2
 *     String recordType = (String) record.getField(2);
 *     switch (recordType) {
 *         case "USER_EVENT":
 *             return new TableRef("events", "user_events");
 *         case "SYSTEM_EVENT":
 *             return new TableRef("events", "system_events");
 *         case "ERROR_EVENT":
 *             return new TableRef("monitoring", "error_logs");
 *         default:
 *             return new TableRef("events", "unknown_events");
 *     }
 * };
 *
 * }</pre>
 *
 * <p><strong>When to Use:</strong> Use TableResolver in DYNAMIC mode when you need to route records
 * to different tables based on record content, time-based partitioning, tenant isolation, or
 * sharding strategies.
 *
 * @param <Input> the input record type
 */
@PublicEvolving
@FunctionalInterface
public interface TableResolver<Input> extends Serializable {

    /**
     * Determines the target table for the given record.
     *
     * @param record the input record
     * @return the table reference containing keyspace and table name
     */
    TableRef resolve(Input record);
}
