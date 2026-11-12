---
title: Cassandra
weight: 4
type: docs
aliases:
  - /zh/dev/connectors/cassandra.html
  - /zh/apis/streaming/connectors/cassandra.html
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

# Apache Cassandra Connector

This connector provides sinks that writes data into a [Apache Cassandra](https://cassandra.apache.org/) database.

The connector uses the [Java Driver for Apache Cassandra](https://apache.github.io/cassandra-java-driver/4.19.0/) in version 4.19.4, which is compatible with Apache Cassandra 2.1 and higher.

To use this connector, add the following dependency to your project:

{{< artifact flink-connector-cassandra withScalaVersion >}}

Note that the streaming connectors are currently __NOT__ part of the binary distribution. See how to link with them for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

## Installing Apache Cassandra
There are multiple ways to bring up a Cassandra instance on local machine:

1. Follow the instructions from [Cassandra Getting Started page](https://cassandra.apache.org/doc/latest/cassandra/getting-started/index.html).
2. Launch a container running Cassandra from [Official Docker Repository](https://hub.docker.com/_/cassandra/)

## Cassandra Source
Flink provides a [FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface)
bounded source to read from Cassandra and return a collection of entities as `DataStream<Entity>`.
An entity is built by the connector based on a POJO containing the annotations of the Cassandra Java driver mapper
(as described in [Mapping POJOs](#mapping-pojos)).

To use the source, do the following:

```java
CassandraSessionBuilder sessionBuilder = new CassandraSessionBuilder() {
    @Override
    protected CqlSession buildSession(CqlSessionBuilder builder) {
        return builder.addContactPoint(new InetSocketAddress(HOST, PORT))
                      .withLocalDatacenter(LOCAL_DATACENTER)
                      .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                              .withString(DefaultDriverOption.REQUEST_CONSISTENCY, CL)
                              .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, CONNECT_TIMEOUT)
                              .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, REQUEST_TIMEOUT)
                              .build())
                      .build();
    }
};
MemorySize maxSplitMemorySize = ... //optional max split size, minimum is 10MB. If not set, maxSplitMemorySize = 64 MB
Source cassandraSource = CassandraSource.builder()
                                        .setSessionBuilder(sessionBuilder)
                                        .setMaxSplitMemorySize(maxSplitMemorySize)
                                        .setQuery("select ... from KEYSPACE.TABLE ...;")
                                        .buildForPojo(Pojo.class);
DataStream<Pojo> stream = env.fromSource(cassandraSource, WatermarkStrategy.noWatermarks(),  "CassandraSource");
```
Regarding performances, the source splits table data like this:
`numSplits = tableSize/maxSplitMemorySize`.

If tableSize cannot be determined or previous numSplits computation makes too few splits, it falls back to `numSplits = parallelism`

## Cassandra Sinks

### Configurations

Flink's Cassandra sink are created by using the static CassandraSink.addSink(DataStream<IN> input) method.
This method returns a CassandraSinkBuilder, which offers methods to further configure the sink, and finally `build()` the sink instance.

The following configuration methods can be used:

1. _setQuery(String query)_
    * Sets the upsert query that is executed for every record the sink receives.
    * The query is internally treated as CQL statement.
    * __DO__ set the upsert query for processing __Tuple__ data type.
    * __DO NOT__ set the query for processing __POJO__ data types.
2. _setSessionBuilder(CassandraSessionBuilder sessionBuilder)_
    * Sets the session builder that is used to configure the connection to cassandra with more sophisticated settings such as consistency level, retry policy and etc. See [Connecting to Cassandra](#connecting-to-cassandra).
3. _setHost(String host[, int port])_
    * Simple version of setSessionBuilder() with host/port information to connect to Cassandra instances
4. _setLocalDatacenter(String localDatacenter)_
    * An __optional__ setting
    * Sets the datacenter of the host set with setHost(). When it is not set, the datacenter is inferred from the host.
5. _setMapperOptions(CassandraMapperOptions options)_
    * Sets the mapper options that are used to configure how POJOs are written (TTL, timestamp, consistency level, tracing, saving null fields, `IF NOT EXISTS`).
    * Only applies when processing __POJO__ data types.
6. _setMaxConcurrentRequests(int maxConcurrentRequests, Duration timeout)_
    * Sets the maximum allowed number of concurrent requests with a timeout for acquiring permits to execute.
    * Only applies when __enableWriteAheadLog()__ is not configured.
7. _enableWriteAheadLog([CheckpointCommitter committer])_
    * An __optional__ setting
    * Allows exactly-once processing for non-deterministic algorithms.
8. _setFailureHandler([CassandraFailureHandler failureHandler])_
    * An __optional__ setting
    * Sets the custom failure handler.
9. _setDefaultKeyspace(String keyspace)_
    * Sets the default keyspace to be used.
10. _enableIgnoreNullFields()_
    * Enables ignoring null values, treats null values as unset and avoids writing null fields and creating tombstones.
11. _build()_
    * Finalizes the configuration and constructs the CassandraSink instance.

### Write-ahead Log

A checkpoint committer stores additional information about completed checkpoints
in some resource. This information is used to prevent a full replay of the last
completed checkpoint in case of a failure.
You can use a `CassandraCommitter` to store these in a separate table in cassandra.
Note that this table will NOT be cleaned up by Flink.

Flink can provide exactly-once guarantees if the query is idempotent (meaning it can be applied multiple
times without changing the result) and checkpointing is enabled. In case of a failure the failed
checkpoint will be replayed completely.

Furthermore, for non-deterministic programs the write-ahead log has to be enabled. For such a program
the replayed checkpoint may be completely different than the previous attempt, which may leave the
database in an inconsistent state since part of the first attempt may already be written.
The write-ahead log guarantees that the replayed checkpoint is identical to the first attempt.
Note that that enabling this feature will have an adverse impact on latency.

<p style="border-radius: 5px; padding: 5px" class="bg-danger"><b>Note</b>: The write-ahead log functionality is currently experimental. In many cases it is sufficient to use the connector without enabling it. Please report problems to the development mailing list.</p>

### Checkpointing and Fault Tolerance
With checkpointing enabled, Cassandra Sink guarantees at-least-once delivery of action requests to C* instance.

More details on [checkpoints docs]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}) and [fault tolerance guarantee docs]({{< ref "docs/connectors/datastream/guarantees" >}})

## Connecting to Cassandra

The sources and the sinks create their connection with a `CassandraSessionBuilder`. Its `buildSession` method
receives the `CqlSessionBuilder` of the driver and returns the `CqlSession`:

```java
CassandraSessionBuilder sessionBuilder = new CassandraSessionBuilder() {
    @Override
    protected CqlSession buildSession(CqlSessionBuilder builder) {
        return builder.addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                      .withLocalDatacenter("datacenter1")
                      .withAuthCredentials("user", "password")
                      .build();
    }
};
```

The driver requires the local datacenter when contact points are set, it only sends requests to the nodes of this
datacenter. `CassandraSink#setHost` and the SQL connector infer the datacenter from the contact points when
`setLocalDatacenter` or the `local-datacenter` option is not set.

### Driver configuration

All the options of the driver (timeouts, consistency levels, execution profiles, load balancing, retries,
address translation, request throttling, SSL, ...) are described in the
[configuration manual](https://apache.github.io/cassandra-java-driver/4.19.0/core/configuration/) of the driver. They can be set in several ways:

* programmatically in `buildSession`, with `builder.withConfigLoader(DriverConfigLoader.programmaticBuilder()...build())`,
* with an `application.conf` file in the classpath of the job,
* with system properties prefixed by `datastax-java-driver.`,
* for the SQL connector, with table options prefixed by `driver.`, for example `'driver.basic.request.page-size' = '1000'`.
  Options whose value is a list cannot be set this way.

The metrics of the driver are disabled by default and are not integrated with Flink metrics.

### Connecting to a cloud database

A cloud database such as [**DataStax Astra**](https://docs.datastax.com/en/astra-db-serverless/index.html) is reached with its secure connect bundle, which contains the contact
points, the local datacenter and the certificates of the database. The bundle is set with
`builder.withCloudSecureConnectBundle(...)` in `buildSession`, or with the `secure-connect-bundle` option of the
SQL connector, as a path of the local filesystem or a URL. It must be accessible from all the nodes of the Flink cluster.

```java
CassandraSessionBuilder sessionBuilder = new CassandraSessionBuilder() {
    @Override
    protected CqlSession buildSession(CqlSessionBuilder builder) {
        return builder.withCloudSecureConnectBundle(Paths.get("/path/to/secure-connect-database_name.zip"))
                      .withAuthCredentials("token", "AstraCS:redacted")
                      .build();
    }
};
```

When a bundle is provided, the contact points and the local datacenter are ignored, including the `hosts`, `port` and
`local-datacenter` options of the SQL connector. The SSL connection is configured from the bundle and the default
consistency level of the driver becomes `LOCAL_QUORUM`.

## Mapping POJOs

The POJO source, the POJO sink and the POJO input and output formats map classes annotated with the
[mapper annotations](https://apache.github.io/cassandra-java-driver/4.19.0/mapper/entities/) of the driver. The connector maps the classes at runtime,
the annotation processor of the driver mapper is not needed. The following annotations are supported:

* `@Entity` is required on the class, `defaultKeyspace` sets the keyspace of the table.
* `@CqlName` sets the name of the table on the class and the name of a column on a field or a getter.
* `@NamingStrategy` sets how the names without `@CqlName` are derived from the Java names. By default,
  they are converted to snake case, for example the property `batchId` is mapped to the column `batch_id`.
* `@Transient` and `@TransientProperties` exclude properties.
* `@Computed` marks a property that is read but never written.

The class must have a public no-argument constructor and each mapped property needs a getter and a setter.
A property whose type is itself annotated with `@Entity` is mapped to a user defined type, collections of
such types are not supported. Custom codecs are registered in `buildSession` with `builder.addTypeCodecs(...)`.

## Migrating from the version 3 of the driver

The versions of the connector up to 3.x use the version 3 of the driver, which has a different API.
The following changes are needed when upgrading:

* `ClusterBuilder#buildCluster(Cluster.Builder)` is replaced by `CassandraSessionBuilder#buildSession(CqlSessionBuilder)`
  and `setClusterBuilder` by `setSessionBuilder`. The SQL option `cluster-builder-class` is renamed to `session-builder-class`.
* The local datacenter has to be set when contact points are set, see [Connecting to Cassandra](#connecting-to-cassandra).
* `MapperOptions` and `SimpleMapperOptions` are replaced by `CassandraMapperOptions`.
* The mapper annotations `@Table` and `@Column` are replaced by `@Entity` and `@CqlName`. A column without `@CqlName`
  is now named after the property converted to snake case.
* CQL `date`, `time` and `timestamp` are mapped to `java.time.LocalDate`, `java.time.LocalTime` and `java.time.Instant`,
  see [temporal types](https://apache.github.io/cassandra-java-driver/4.19.0/core/temporal_types/). `java.util.Date` is still supported for `timestamp` columns.
  Tuples read with `CassandraInputFormat` contain the `java.time` types.
* The request timeout of the driver, set by the SQL option `read-timeout`, now covers a request including its retries.
* The exceptions of the driver moved to the packages `com.datastax.oss.driver.api.core` and
  `com.datastax.oss.driver.api.core.servererrors`, this impacts the implementations of `CassandraFailureHandler`.
  `NoHostAvailableException` is replaced by `AllNodesFailedException`.
* The driver does not expose Guava anymore, `CassandraSinkBase#send` returns a `CompletionStage`.

## Examples

The Cassandra sink currently supports both Tuple and POJO data types, and Flink automatically detects which type of input is used. For general use of those streaming data types, please refer to [Supported Data Types]({{< ref "docs/dev/datastream/fault-tolerance/serialization/types_serialization" >}}#supported-data-types). We show two implementations based on {{< gh_link file="flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/socket/SocketWindowWordCount.java" name="SocketWindowWordCount" >}}, for POJO and Tuple data types respectively.

In all these examples, we assumed the associated Keyspace `example` and Table `wordcount` have been created.

{{< tabs "ffc5c4d4-7872-479c-bfa6-206b9e96f6f3" >}}
{{< tab "CQL" >}}
```sql
CREATE KEYSPACE IF NOT EXISTS example
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE IF NOT EXISTS example.wordcount (
    word text,
    count bigint,
    PRIMARY KEY(word)
);
```
{{< /tab >}}
{{< /tabs >}}

### Cassandra Sink Example for Streaming Tuple Data Type
While storing the result with Java/Scala Tuple data type to a Cassandra sink, it is required to set a CQL upsert statement (via setQuery('stmt')) to persist each record back to the database. With the upsert query cached as `PreparedStatement`, each Tuple element is converted to parameters of the statement.

For details about `PreparedStatement` and `BoundStatement`, please visit the [Java Driver manual](https://apache.github.io/cassandra-java-driver/4.19.0/core/statements/prepared/)

{{< tabs "1a84c6a0-0b2f-4f96-8cf8-43ec6dd3bc5d" >}}
{{< tab "Java" >}}
```java
// get the execution environment
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// get input data by connecting to the socket
DataStream<String> text = env.socketTextStream(hostname, port, "\n");

// parse the data, group it, window it, and aggregate the counts
DataStream<Tuple2<String, Long>> result = text
        .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
                // normalize and split the line
                String[] words = value.toLowerCase().split("\\s");

                // emit the pairs
                for (String word : words) {
                    //Do not accept empty word, since word is defined as primary key in C* table
                    if (!word.isEmpty()) {
                        out.collect(new Tuple2<String, Long>(word, 1L));
                    }
                }
            }
        })
        .keyBy(value -> value.f0)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        .sum(1);

CassandraSink.addSink(result)
        .setQuery("INSERT INTO example.wordcount(word, count) values (?, ?);")
        .setHost("127.0.0.1")
        .build();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

// get input data by connecting to the socket
val text: DataStream[String] = env.socketTextStream(hostname, port, '\n')

// parse the data, group it, window it, and aggregate the counts
val result: DataStream[(String, Long)] = text
  // split up the lines in pairs (2-tuples) containing: (word,1)
  .flatMap(_.toLowerCase.split("\\s"))
  .filter(_.nonEmpty)
  .map((_, 1L))
  // group by the tuple field "0" and sum up tuple field "1"
  .keyBy(_._1)
  .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
  .sum(1)

CassandraSink.addSink(result)
  .setQuery("INSERT INTO example.wordcount(word, count) values (?, ?);")
  .setHost("127.0.0.1")
  .build()

result.print().setParallelism(1)
```
{{< /tab >}}
{{< /tabs >}}


### Cassandra Sink Example for Streaming POJO Data Type
An example of streaming a POJO data type and store the same POJO entity back to Cassandra. In addition, this POJO implementation needs to be annotated as described in [Mapping POJOs](#mapping-pojos), as each field of this entity is mapped to an associated column of the designated table.

The mapping of each table column can be defined through annotations placed on a field declaration in the Pojo class.  For details of the mapping, please refer to the Java Driver manual on [Entities](https://apache.github.io/cassandra-java-driver/4.19.0/mapper/entities/) and [CQL Data types](https://cassandra.apache.org/doc/latest/cassandra/developing/cql/types.html)

{{< tabs "d65ca6f5-acb2-4f2c-b5b6-d986eafca765" >}}
{{< tab "Java" >}}
```java
// get the execution environment
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// get input data by connecting to the socket
DataStream<String> text = env.socketTextStream(hostname, port, "\n");

// parse the data, group it, window it, and aggregate the counts
DataStream<WordCount> result = text
        .flatMap(new FlatMapFunction<String, WordCount>() {
            public void flatMap(String value, Collector<WordCount> out) {
                // normalize and split the line
                String[] words = value.toLowerCase().split("\\s");

                // emit the pairs
                for (String word : words) {
                    if (!word.isEmpty()) {
                        //Do not accept empty word, since word is defined as primary key in C* table
                        out.collect(new WordCount(word, 1L));
                    }
                }
            }
        })
        .keyBy(WordCount::getWord)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))

        .reduce(new ReduceFunction<WordCount>() {
            @Override
            public WordCount reduce(WordCount a, WordCount b) {
                return new WordCount(a.getWord(), a.getCount() + b.getCount());
            }
        });

CassandraSink.addSink(result)
        .setHost("127.0.0.1")
        .setMapperOptions(new CassandraMapperOptions().saveNullFields(true))
        .build();


@Entity(defaultKeyspace = "example")
@CqlName("wordcount")
public class WordCount {

    @CqlName("word")
    private String word = "";

    @CqlName("count")
    private long count = 0;

    public WordCount() {}

    public WordCount(String word, long count) {
        this.setWord(word);
        this.setCount(count);
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return getWord() + " : " + getCount();
    }
}
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
