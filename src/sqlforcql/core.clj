(ns sqlforcql.core
  (:require [sqlforcql.db :as db]))

(defn connect-to-default-db
  "Connect to a Cassandra keyspace on a particular ip-address, with the given username and password.
  This information is stored in atoms/default-db-map map."
  [ip-address username password keyspace]
  (let [db-map (db/connect-to-db ip-address username password keyspace)]
    (reset! db/default-db-map db-map)))

(defn disconnect-from-default-db
  "Disconnect from Cassandra using information stored in atoms/default-db-map map.
  Afterwards, empty the atoms/default-db-map map."
  []
  (db/disconnect-from-db @db/default-db-map)
  (reset! db/default-db-map (db/get-empty-db-map)))

(comment
  (require '[sqlforcql.core :as core])
  (core/connect-to-default-db "localhost" "username" "password" "keyspace")
  (core/disconnect-from-default-db))
