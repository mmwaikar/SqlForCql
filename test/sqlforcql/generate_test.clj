(ns sqlforcql.generate-test
  (:require [clojure.test :refer :all]
            [taoensso.timbre :refer [debug info]]
            [fipp.edn :refer [pprint] :rename {pprint fipp}]
            [sqlforcql.config :as config]
            [sqlforcql.analyze :as analyze]
            [sqlforcql.generate :as generate]))

(comment
  (use 'sqlforcql.generate-test)
  (clojure.test/run-tests 'sqlforcql.generate-test)
  )

;; the below statement automatically wraps all the tests to connect
;; to the db, run tests and then disconnect from the db.
(use-fixtures :once config/db-test-fixture)

(deftest should-get-insert-statements
  (testing "Should get the insert statements for rows which differ in the main vs. the supporting query table."
    (let [diff (analyze/get-diff "players" "players_by_city")
          table-name (first (keys diff))
          table-rows (first (vals diff))
          insert-statements (if-not (= table-name :no-difference)
                              (generate/get-insert-statements table-name table-rows)
                              (sequence ()))]
      (fipp insert-statements)
      (is (= 1 (count insert-statements))))))