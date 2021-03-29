(ns sqlforcql.analyze-test
  (:require [clojure.test :refer :all]
            [taoensso.timbre :refer [debug info]]
            [sqlforcql.config :as config]
            [sqlforcql.analyze :as analyze]))

(comment
  (use 'sqlforcql.analyze-test)
  (clojure.test/run-tests 'sqlforcql.analyze-test)
  )

;; the below statement automatically wraps all the tests to connect
;; to the db, run tests and then disconnect from the db.
(use-fixtures :once config/db-test-fixture)

(deftest should-get-counts
  (testing "Should get counts of multiple tables."
    (let [name-count-map (analyze/get-counts ["artifacts" "artifacts_by_container_key"])]
      (info "counts:" name-count-map)
      (is (= 2 (count (keys name-count-map)))))))

(deftest should-get-diff
  (testing "Should get the difference in number of rows in a main table vs. it's supporting query table."
    (let [diff (analyze/get-diff "container_types" "container_types_by_external_id")]
      (info "diff:" diff)
      (is (empty? (first (vals diff)))))))