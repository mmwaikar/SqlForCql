# SqlForCql

SQL like capabilities for Apache Cassandra.

This library is born out of frustration of expecting SQL (kind of) queries to work against Cassandra, but they don't. 
The situation becomes worse when you have to update some data.

Recently, I was working on a data migration task, and due to some logical errors, I had to rollback the changes made to
the data, but, sadly, this was not an RDBMS!

Hence, this library, which helps in executing those queries against a Cassandra database which are not possible using 
plain CQL. Seemingly simple queries, e.g. a query with a `like` clause or, a query where we have to update multiple
rows (which possibly, could've been achieved using a nested SQL query), are made possible using this library.

This library is written in Clojure, so the syntax is far from that of SQL, but it is minimal enough to be usable.

## Usage

[![Clojars Project](https://img.shields.io/clojars/v/sqlforcql.svg)](https://clojars.org/sqlforcql)

There are just a few namespaces in this library:

1. **sqlforcql.core** - with functions to connect to and disconnect from Cassandra
2. **sqlforcql.cql** - with functions to get / update data from Cassandra
3. **sqlforcql.db** - which actually establishes a connection to Cassandra and disconnects from it
4. **sqlforcql.atoms** - contains a single `default-db-map` atom which stores Cassandra connection parameters
5. **sqlforcql.querybuilder** - which constructs valid CQL queries to be run against Cassandra
6. **sqlforcql.analyze** - with a function to get counts of various tables, and a function to get the difference in the 
   rows of a main table, and it's supporting query table.

Before we can start executing any queries, we need to connect to a Cassandra instance. This can be done using:
```
  (core/connect-to-default-db "localhost" "username" "password" "keyspace")
```

Once we are done, we can disconnect from Cassandra using:
```
  (core/disconnect-from-default-db)
```

### Sample schema

The tests in this library `sqlforcql.schema` create the following two tables (with the data as shown):

#### players table

*with nickname as the partitioning key (hereafter, referred to as the PK column)*

|nickname(*)|city     |country    |first_name |last_name|zip   |
|-----------|---------|-----------|-----------|---------|------|
|fedex      |Bern     |Switzerland|Roger      |Federer  |3001  |
|naseer     |Ajmer    |India      |Naseeruddin|Shah     |305001|
|sonu       |Dubai    |UAE        |Sonu       |Nigam    |00000 |
|rafa       |Madrid   |Spain      |Rafael     |Nadal    |28001 |
|king       |Abu Dhabi|UAE        |Shahrukh   |Khan     |00000 |
|chintu     |Jodhpur  |India      |Rishi      |Kapoor   |305001|

#### players_by_city table

*with city and country as the partitioning keys, so country becomes the clustering column (hereafter, referred to as 
the CK column)*

|city(*)  |country(*) |first_name |last_name  |nickname|zip   |
|---------|-----------|-----------|-----------|--------|------|
|Jodhpur  |India      |Rishi      |Kapoor     |chintu  |305001|
|Bern     |Switzerland|Roger      |Federer    |fedex   |3001  |
|Abu Dhabi|UAE        |Shahrukh   |Khan       |king    |00000 |
|Ajmer    |India      |Naseeruddin|Shah       |naseer  |305001|
|Dubai    |UAE        |Sonu       |Nigam      |sonu    |00000 |
|Madrid   |Spain      |Rafael     |Nadal      |rafa    |28001 |

#### The following queries are obvious (though nothing which CQL can't do):

* select * from players; - `(cql/get-all "players")`

* select count(*) from players; - `(cql/get-count "players")`

* select * from players where city = 'Ajmer' allow filtering; `(cql/get-by-non-pk-col "players" {:city "Ajmer"})`

* select * from players where nickname = 'fedex'; `(cql/get-by-pk-col "players" {:nickname "fedex"})`

#### Or, from players_by_city table where city and country are PK / CK columns respectively:

* select * from players_by_city where city = 'Jodhpur' and country = 'India'; 
`(cql/get-by-pk-col "players_by_city" {:city "Jodhpur" :country "India"})`

#### Now this is where (_the CQL betrays us_) and the fun starts:

_Suppose we wanted to execute a SQLish query with a like clause:_
* select * from players where city like 'Dhabi'; `(cql/get-by-non-pk-col-like "players" {:city "Dhabi"})`

_Or, suppose we wanted to update multiple rows based on a criteria involving some non-PK column:_
* update players set city = 'X' where city = 'Y'; `(cql/update-by-non-pk-col "players" :nickname {:city "X"} {:city "Y"})`

_A query similar to the above update query against a table having both PK and CK columns, we have to use:_
* update players_by_city set zip = 411038 where zip = 305001; - `(cql/update-by-non-pk-col-with-clustering-col "players_by_city" [:city :country] {:zip 305001} {:zip 411038})`

#### Getting counts of the number of rows of a few tables:
* `(analyze/get-counts ["players" "players_by_city"])`

#### Getting difference in the rows of a main table, and it's supporting query table:
* `(analyze/get-diff "players" "players_by_city")`

## License

Copyright © 2020 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
