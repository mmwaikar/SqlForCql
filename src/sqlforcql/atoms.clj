(ns sqlforcql.atoms
  (:require [sqlforcql.db :as db]))

(def default-db-map
  "An atom which is a map, which stores the Cassandra cluster, session and keyspace connected to."
  (atom (db/get-db-map nil nil "keyspace-name")))

(defn db-map-empty?
  "Determines if the default-db-map atom is empty or not."
  []
  (if (and (nil? (:session @default-db-map))
           (nil? (:keyspace @default-db-map)))
    true
    false))
