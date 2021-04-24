(ns sqlforcql.specs-test
  (:require [clojure.test :refer :all]
            [clojure.spec.alpha :as s]
            [sqlforcql.specs :as specs]))

(comment
  (use 'sqlforcql.specs-test)
  (clojure.test/run-tests 'sqlforcql.specs-test)
  )

(deftest should-validate-name-or-with-keyspace
  (testing "Should validate name-or-with-keyspace spec."
    (is (= (s/valid? ::specs/name-or-with-keyspace ["tn"]) true) "only table name invalid")
    (is (= (s/valid? ::specs/name-or-with-keyspace ["ks" "tn"]) true) "keyspace, table name invalid")))

(deftest should-validate-name-or-with-keyspace-pk-col-val-map
  (testing "Should validate name-or-with-keyspace-pk-col-val-map spec."
    (is (= (s/valid? ::specs/name-or-with-session-keyspace-pk-col-val-map ["tn" {:n "v"}]) true) "table name, pk col map invalid")
    (is (= (s/valid? ::specs/name-or-with-session-keyspace-pk-col-val-map ["ks" "tn" {:n "v"}]) true) "keyspace, table name, pk col map invalid")))

(deftest should-validate-name-or-with-session-keyspace
  (testing "Should validate name-or-with-keyspace spec."
    (is (= (s/valid? ::specs/name-or-with-session-keyspace ["tn"]) true) "only table name invalid")
    (is (= (s/valid? ::specs/name-or-with-session-keyspace ["s" "tn"]) true) "session, table name invalid")
    (is (= (s/valid? ::specs/name-or-with-session-keyspace ["s" "k" "tn"]) true) "session, keyspace, table name invalid")))

(deftest should-validate-names-or-with-session-keyspace
  (testing "Should validate names-or-with-session-keyspace spec."
    (is (= (s/valid? ::specs/names-or-with-session-keyspace [["tn1" "tn2"]]) true) "only table name invalid")
    (is (= (s/valid? ::specs/names-or-with-session-keyspace ["s" ["tn1" "tn2"]]) true) "session, table name invalid")
    (is (= (s/valid? ::specs/names-or-with-session-keyspace ["s" "k" ["tn1" "tn2"]]) true) "session, keyspace, table name invalid")))

(deftest should-validate-two-names-or-with-session-keyspace
  (testing "Should validate two-names-or-with-session-keyspace spec."
    (is (= (s/valid? ::specs/two-names-or-with-session-keyspace ["tn1" "tn2"]) true) "only table name invalid")
    (is (= (s/valid? ::specs/two-names-or-with-session-keyspace ["s" "tn1" "tn2"]) true) "session, table name invalid")
    (is (= (s/valid? ::specs/two-names-or-with-session-keyspace ["s" "k" "tn1" "tn2"]) true) "session, keyspace, table name invalid")))
