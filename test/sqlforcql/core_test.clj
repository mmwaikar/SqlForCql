(ns sqlforcql.core-test
  (:require [clojure.test :refer :all]
            [taoensso.timbre :refer [debug info]]
            [sqlforcql.cql :as cql]
            [sqlforcql.config :as config]))

(def players-table (atom "players"))
(def players-by-city-table (atom "players_by_city"))

(def players-pk-col (atom :nickname))
(def players-non-pk-col (atom :city))

(def players-by-city-pk-col (atom :city))
(def players-by-city-ck-col (atom :country))
(def players-by-city-non-pk-col (atom :city))

;; the below statement automatically wraps all the tests to connect
;; to the db, run tests and then disconnect from the db.
(use-fixtures :once config/db-test-fixture)

(deftest should-get-all
  (testing "Should load all data from a table."
    (let [data (cql/get-all @players-table)
          found (count data)]
      (info "Found" found "record(s) using get-all fn.")
      (is (= 6 found)))))

(deftest should-get-by-partitioning-key-column
  (testing "Should get a record by the partitioning key column."
    (let [by-pk (cql/get-by-pk-col @players-table {@players-pk-col "rafa"})
          found (count by-pk)]
      (info "Found" found "record(s) using get-by-pk-col fn.")
      (is (= 1 found)))))

(deftest should-get-by-partitioning-clustering-key-columns
  (testing "Should get a record by the partitioning key & the clustering key columns."
    (let [by-pk (cql/get-by-pk-col @players-by-city-table
                                   {@players-by-city-pk-col "Ajmer"
                                    @players-by-city-ck-col "India"})
          found (count by-pk)]
      (info "Found" found "record(s) using get-by-pk-col fn (with clustering key).")
      (is (= 1 found)))))

(deftest should-get-by-non-partitioning-key-column
  (testing "Should get a record by a column which is not the partitioning key column."
    (let [by-pk (cql/get-by-non-pk-col @players-table {@players-non-pk-col "Abu Dhabi"})
          found (count by-pk)]
      (info "Found" found "record(s) using get-by-non-pk-col fn.")
      (is (= 2 found)))))

(deftest should-get-by-non-partitioning-key-column-using-a-sql-like-clause
  (testing "Should get a record by a column which is not the partitioning key column and the value contains a string."
    (let [by-pk (cql/get-by-non-pk-col-like @players-table {@players-non-pk-col "Dhabi"})
          found (count by-pk)]
      (info "Found" found "record(s) using get-by-non-pk-col-like fn.")
      (is (= 2 found)))))
