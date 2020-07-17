(ns sqlforcql.core
  (:require [sqlforcql.db :as db]
            [sqlforcql.atoms :as atoms]))

(defn connect-to-default-db
  ([] (connect-to-default-db "keyspace-name"))

  ([keyspace] (let [db-map (db/connect-to-db "localhost" "cassandra" "cassandra" keyspace)]
                (reset! atoms/default-db-map db-map)))

  ([ip-address username password keyspace]
   (let [db-map (db/connect-to-db ip-address username password keyspace)]
     (reset! atoms/default-db-map db-map))))

(defn disconnect-from-default-db []
  (db/disconnect-from-db (deref atoms/default-db-map)))
