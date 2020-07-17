(ns sqlforcql.config
  (:require [clojure.test :refer :all]
            [sqlforcql.cql :as cql]
            [sqlforcql.core :as core]
            [sqlforcql.atoms :as atoms]))

(defn db-test-fixture
  "Connects to the default db, uses the default session and then disconnects from the db."
  [f]
  (core/connect-to-default-db "sqlforcql")
  (let [{session :session
         keyspace :keyspace} (deref atoms/default-db-map)]
    (cql/set-db-map! session keyspace))
  (f)
  (core/disconnect-from-default-db))