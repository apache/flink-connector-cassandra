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

package org.apache.flink.batch.connectors.cassandra;

import org.apache.flink.connector.cassandra.mapper.PojoMapper;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.connectors.cassandra.CassandraMapperOptions;
import org.apache.flink.streaming.connectors.cassandra.CassandraSessionBuilder;
import org.apache.flink.util.Preconditions;

import com.datastax.oss.driver.api.core.cql.Row;

import java.util.Iterator;

/**
 * InputFormat to read data from Apache Cassandra and generate a custom Cassandra annotated object.
 *
 * @param <OUT> type of inputClass
 */
public class CassandraPojoInputFormat<OUT> extends CassandraInputFormatBase<OUT> {

    private static final long serialVersionUID = 1992091320180905115L;

    private transient Iterator<Row> rows;
    private transient PojoMapper<OUT> mapper;
    private final Class<OUT> inputClass;

    public CassandraPojoInputFormat(
            String query, CassandraSessionBuilder builder, Class<OUT> inputClass) {
        this(query, builder, inputClass, null);
    }

    /**
     * Creates the input format.
     *
     * @param mapperOptions the mapper options, they only configure writes and are not used by this
     *     input format
     */
    public CassandraPojoInputFormat(
            String query,
            CassandraSessionBuilder builder,
            Class<OUT> inputClass,
            CassandraMapperOptions mapperOptions) {
        super(query, builder);
        this.inputClass = Preconditions.checkNotNull(inputClass, "InputClass cannot be null");
    }

    @Override
    public void open(InputSplit split) {
        this.session = builder.getSession();
        this.mapper = PojoMapper.create(inputClass);
        this.rows = session.execute(query).iterator();
    }

    @Override
    public boolean reachedEnd() {
        return !rows.hasNext();
    }

    @Override
    public OUT nextRecord(OUT reuse) {
        return mapper.map(rows.next());
    }
}
