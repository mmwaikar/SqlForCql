(ns sqlforcql.core-test
  (:require [clojure.test :refer :all]
            [taoensso.timbre :refer [debug info]]
            [sqlforcql.cql :as cql]
            [sqlforcql.config :as config]))

(def table (atom "players"))
(def pk-col (atom :nickname))

(use-fixtures :once config/db-test-fixture)

(deftest should-get-all
  (testing "Should load all data from a table."
    (let [data (cql/get-all @table)
          found (count data)]
      (info "Found" found "records using get-all fn.")
      (is (= 6 found)))))

(deftest should_get-by-partitioning-key
  (testing "Should get a record by the partitioning key."
    (let [by-pk (cql/get-by-pk-col @table {@pk-col "rafa"})
          found (count by-pk)]
      (info "Found" found "records using get-by-pk-col fn.")
      (is (= 1 found)))))
