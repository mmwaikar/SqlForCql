(ns sqlforcql.config
  (:require [clojure.test :refer :all]
            [sqlforcql.core :as core]))

(defn db-test-fixture
  "Connects to the default db, uses the default session and then disconnects from the db."
  [f]
  (core/connect-to-default-db "sqlforcql")
  (f)
  (core/disconnect-from-default-db))