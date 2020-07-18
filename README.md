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

There are just a few namespaces in this library:

1. **sqlforcql.core** - with functions to connect to and disconnect from Cassandra
2. **sqlforcql.cql** - with functions to get / update data from Cassandra
3. **sqlforcql.db** - which actually establishes a connection to Cassandra and disconnects from it
4. **sqlforcql.atoms** - contains a single `default-db-map` atom which stores Cassandra connection parameters
5. **sqlforcql.querybuilder** - which constructs valid CQL queries to be run against Cassandra

Before we can start executing any queries, we need to connect to a Cassandra instance. This can be done using:
```
  (core/connect-to-default-db "localhost" "username" "password" "keyspace")
```

Once we are done, we can disconnect from Cassandra using:
```
  (core/disconnect-from-default-db)
```

FIXME

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
