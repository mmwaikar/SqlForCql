(ns sqlforcql.atoms
  (:require [sqlforcql.db :as db]))

(def default-db-map (atom (db/get-db-map nil nil "keyspace-name")))
