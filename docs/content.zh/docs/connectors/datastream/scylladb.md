---
title: ScyllaDB
weight: 4
type: docs
aliases:
  - /zh/dev/connectors/scylladb.html
  - /zh/apis/streaming/connectors/scylladb.html
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

ScyllaDB is supported by Apache Cassandra Connector just by replacing connection string from running Cassandra to running ScyllaDB.

## Installing ScyllaDB
There are multiple ways to bring up a ScyllaDB instance on local machine:

1. Follow the instructions from [ScyllaDB Getting Started page](https://docs.scylladb.com/getting-started/).
2. Launch a container running ScyllaDB from [Official Docker Repository](https://hub.docker.com/r/scylladb/scylla/)
