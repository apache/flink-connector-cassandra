---
title: ScyllaDB
weight: 4
type: docs
aliases:
  - /dev/connectors/scylladb.html
  - /apis/streaming/connectors/scylladb.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# ScyllaDB Connector

## Overview

The Apache Flink Cassandra Connector fully supports **ScyllaDB** as a drop-in replacement for Apache Cassandra. All connector features work seamlessly with ScyllaDB without requiring any code changes.

**Key Features:**
- Read from ScyllaDB using CassandraSource
- Write to ScyllaDB with at-least-once or exactly-once semantics
- Batch input/output formats for POJO, Tuple, and Row types
- Automatic split generation for parallel processing
- Full CQL query support
- Validated with comprehensive integration tests on ScyllaDB 2025.1.4

## Quick Start

Simply point your Flink Cassandra Connector to ScyllaDB nodes instead of Cassandra:

```java
ClusterBuilder clusterBuilder = new ClusterBuilder() {
    @Override
    protected Cluster buildCluster(Cluster.Builder builder) {
        return builder.addContactPoint("127.0.0.1")  // ScyllaDB node
                      .withPort(9042)
                      .build();
    }
};
```

No other code changes required!

## Installation

### Adding the Dependency

{{< artifact flink-connector-cassandra withScalaVersion >}}

Note that the streaming connectors are currently __NOT__ part of the binary distribution. See how to link with them for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

### Running ScyllaDB

**Docker (Recommended for Development):**
```bash
# Single node
docker run --name scylla -p 9042:9042 -d scylladb/scylla:2025.1.4

# Multi-node on macOS (requires special flag)
docker run --name scylla -p 9042:9042 -d \
  scylladb/scylla:2025.1.4 --reactor-backend=epoll
```

**Production Deployment:**
- Follow [ScyllaDB Installation Guide](https://docs.scylladb.com/getting-started/)
- Use [ScyllaDB Cloud](https://www.scylladb.com/product/scylla-cloud/) for managed instances

## Usage Examples

### Reading from ScyllaDB

```java
import org.apache.flink.connector.cassandra.source.CassandraSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.datastax.driver.mapping.Mapper;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

CassandraSource<MyPojo> source = new CassandraSource<>(
    clusterBuilder,
    MyPojo.class,
    "SELECT * FROM mykeyspace.mytable;",
    () -> new Mapper.Option[] {Mapper.Option.saveNullFields(true)}
);

env.fromSource(source, WatermarkStrategy.noWatermarks(), "ScyllaDB Source").print();
```

### Writing to ScyllaDB

```java
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;

// Assuming you have a DataStream of POJOs
CassandraSink.addSink(dataStream)
    .setClusterBuilder(clusterBuilder)
    .build();
```

## Configuration

Connection settings are configured through the DataStax driver's `ClusterBuilder`:

```java
ClusterBuilder clusterBuilder = new ClusterBuilder() {
    @Override
    protected Cluster buildCluster(Cluster.Builder builder) {
        return builder
            .addContactPoints("scylla-node1", "scylla-node2")
            .withPort(9042)
            .withQueryOptions(new QueryOptions()
                .setConsistencyLevel(ConsistencyLevel.QUORUM))
            .withSocketOptions(new SocketOptions()
                .setConnectTimeoutMillis(15000)
                .setReadTimeoutMillis(36000))
            .build();
    }
};
```

## Migration from Cassandra

To switch from Cassandra to ScyllaDB:

1. **Update connection configuration** - Point to ScyllaDB nodes
2. **No code changes required** - All APIs remain identical
3. **Test thoroughly** - Validate with your workload

That's it! The connector handles all CQL communication identically for both databases.

## Compatibility

- **Tested ScyllaDB Version:** 2025.1.4 (open source)
- **CQL Protocol:** 100% compatible with Cassandra 4.x
- **DataStax Driver:** 3.11.2
- **Known Limitations:** None - all connector features work with ScyllaDB

## Additional Resources

- [Cassandra Connector Documentation](cassandra.md) - Full API reference
- [ScyllaDB Documentation](https://docs.scylladb.com/)
- [DataStax Driver Documentation](https://docs.datastax.com/en/developer/java-driver/3.11/)
