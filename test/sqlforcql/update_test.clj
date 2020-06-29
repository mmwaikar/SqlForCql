(ns sqlforcql.update-test
  (:require [clojure.test :refer :all]
            [taoensso.timbre :refer [debug info]]
            [sqlforcql.cql :as cql]
            [sqlforcql.config :as config]
            [sqlforcql.atoms-test :as test-atoms]))

;; the below statement automatically wraps all the tests to connect
;; to the db, run tests and then disconnect from the db.
(use-fixtures :once config/db-test-fixture)

(deftest should-update-rows-by-non-partitioning-key-column
  (testing "Should update rows by a column which is not the partitioning key column."
    ;; since update does not return any rows, we need to fire the select query again
    (let [current-value "Shabi"
          updated-value "Abu Dhabi"
          ;current-value "Abu Dhabi"
          ;updated-value "Shabi"
          do-update (cql/update-by-non-pk-col @test-atoms/players-table
                                                 @test-atoms/players-pk-col
                                                 {@test-atoms/players-non-pk-col current-value}
                                                 {@test-atoms/players-non-pk-col updated-value})
          updated-rows (cql/get-by-non-pk-col @test-atoms/players-table
                                              {@test-atoms/players-non-pk-col updated-value})
          updated (count updated-rows)]
      (info "Updated" updated "record(s) using update-by-non-pk-col fn.")
      (is (= 2 updated)))))
