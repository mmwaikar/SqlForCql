(ns sqlforcql.update-test
  (:require [clojure.test :refer :all]
            [taoensso.timbre :refer [debug info]]
            [sqlforcql.cql :as cql]
            [sqlforcql.config :as config]
            [sqlforcql.atoms-test :as test-atoms]))

(deftest should-update-rows-by-non-partitioning-key-column
  (testing "Should update rows by a column which is not the partitioning key column."
    (let [updated-rows (cql/update-by-non-pk-col @test-atoms/players-table
                                                 @test-atoms/players-pk-col
                                                 {@test-atoms/players-non-pk-col "Abu Dhabi"}
                                                 {@test-atoms/players-non-pk-col "Shabi"})
          updated (count updated-rows)]
      (info "Updated" updated "record(s) using update-by-non-pk-col fn.")
      (is (= 1 updated)))))
