(ns sqlforcql.core
  (:require [sqlforcql.db :as db]
            [sqlforcql.atoms :as atoms]))

(defn connect-to-default-db []
  (let [db-map (db/connect-to-db "localhost" "cassandra" "cassandra" "sqlforcql")]
    (reset! atoms/default-db-map db-map)))

(defn disconnect-from-default-db []
  (db/disconnect-from-db (deref atoms/default-db-map)))

;(defn foo
;  "I don't do a whole lot."
;  [x]
;  (println x "Hello, World!"))
