[![storm-clickhouse](https://maven-badges.herokuapp.com/maven-central/neuronus/storm-clickhouse/badge.svg)](https://maven-badges.herokuapp.com/maven-central/neuronus/storm-clickhouse) ClickHouse JDBC for Apache Storm
===============

This package includes the helper classes for integrate [Apache Storm](http://storm.apache.org) with [ClickHouse JDBC driver](https://github.com/yandex/clickhouse-jdbc) via [Storm JDBC](https://github.com/apache/storm/tree/master/external/storm-jdbc).

## Inserting into a database.
The bolt and trident state included in **Storm JDBC** for inserting data into a database tables are tied to a single table.

### ConnectionProvider
An interface that should be implemented by different connection pooling mechanism `org.apache.storm.jdbc.common.ConnectionProvider`

```java
public interface ConnectionProvider extends Serializable {
    /**
     * method must be idempotent.
     */
    void prepare();

    /**
     *
     * @return a DB connection over which the queries can be executed.
     */
    Connection getConnection();

    /**
     * called once when the system is shutting down, should be idempotent.
     */
    void cleanup();
}
```

We support `org.apache.storm.jdbc.common.ClickhouseJdbcConnectionProvider` which is an implementation that uses HikariCP.

###JdbcMapper
The main API for inserting data in a table using JDBC is the `org.apache.storm.jdbc.mapper.JdbcMapper` interface:

```java
public interface JdbcMapper  extends Serializable {
    List<Column> getColumns(ITuple tuple);
}
```

The `getColumns()` method defines how a storm tuple maps to a list of columns representing a row in a database. 
**The order of the returned list is important. The place holders in the supplied queries are resolved in the same order as returned list.**
For example if the user supplied insert query is `insert into user(user_id, user_name, create_date) values (?,?, now())` the 1st item of the returned list of `getColumns` method will map to the 1st place holder and the 2nd to the 2nd and so on. We do not parse
the supplied queries to try and resolve place holder by column names. Not making any assumptions about the query syntax allows this connector
to be used by some non-standard sql frameworks like Pheonix which only supports upsert into.

### JdbcInsertBolt
To use the `JdbcInsertBolt`, you construct an instance of it by specifying a `ClickhouseJdbcConnectionProvider` and a `JdbcMapper` implementation that converts storm tuple to DB row. In addition, you must either supply
a table name  using `withTableName` method or an insert query using `withInsertQuery`. If you specify a insert query you should ensure that your `JdbcMapper` implementation will return a list of columns in the same order as in your insert query.
You can optionally specify a query timeout seconds param that specifies max seconds an insert query can take. The default is set to value of topology.message.timeout.secs and a value of -1 will indicate not to set any query timeout.
You should set the query timeout value to be <= topology.message.timeout.secs.


```java
Map configMap = Maps.newHashMap();
// Hikari properties
configMap.put("dataSource.url", "jdbc:clickhouse://127.0.0.1:8123/test");
configMap.put("dataSource.user","default");
configMap.put("dataSource.password","");
// ClickHouse properties
configMap.put("dataSource.compress","true");

ConnectionProvider connectionProvider = new ClickhouseJdbcConnectionProvider(configMap);

String tableName = "user_details";
JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                                    .withTableName("user")
                                    .withQueryTimeoutSecs(30);
                                    Or
JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                                    .withInsertQuery("insert into user values (?,?)")
                                    .withQueryTimeoutSecs(30);                                    
```

**Note**: ClickHouse specific JDBC properties should have the prefix `dataSource.` (example for `compress` property: `dataSource.compress=true`).

### Usage
```xml
<dependency>
    <groupId>neuronus</groupId>
    <artifactId>storm-clickhouse</artifactId>
    <version>0.1-SNAPSHOT</version>
</dependency>
```

URL syntax:
`jdbc:clickhouse://<host>:<port>[/<database>]`, e.g. `jdbc:clickhouse://localhost:8123/test`



## License

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


