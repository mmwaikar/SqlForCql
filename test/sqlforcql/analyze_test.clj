(ns sqlforcql.analyze-test
  (:require [clojure.test :refer :all]
            [taoensso.timbre :refer [debug info]]
            [sqlforcql.fixture :as fixture]
            [sqlforcql.analyze :as analyze]))

(comment
  (use 'sqlforcql.analyze-test)
  (clojure.test/run-tests 'sqlforcql.analyze-test)
  )

;; the below statement automatically wraps all the tests to connect
;; to the db, run tests and then disconnect from the db.
(use-fixtures :once fixture/db-test-fixture)

(deftest should-get-counts
  (testing "Should get counts of multiple tables."
    (let [name-count-map (analyze/get-counts ["players" "players_by_city"])]
      (info "counts:" name-count-map)
      (is (= 2 (count (keys name-count-map)))))))

(deftest should-get-diff
  (testing "Should get the difference in number of rows in a main table vs. it's supporting query table."
    (let [diff (analyze/get-diff "players" "players_by_city")]
      (info "diff:" diff)
      (is (empty? (first (vals diff)))))))
