(ns sqlforcql.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen])
  (:import [com.datastax.driver.core SessionManager]))

(defn session? [session]
  (= (type session) SessionManager))

(s/def ::keyspace string?)
(s/def ::table-name string?)
(s/def ::pk-col-name-value-map map?)
(s/def ::is-set set?)

(s/def ::seq-with-single-val (s/and seq? #(= (count %) 1)))

(s/def ::name-or-with-keyspace
  (s/alt :table (s/cat :table-name string?)
         :keyspace-table (s/cat :keyspace string? :table-name string?)))

(s/def ::name-or-with-session-keyspace
  (s/alt :table (s/cat :table-name string?)
         :session-table (s/cat :session session? :table-name string?)
         :session-keyspace-table (s/cat :session session? :keyspace string? :table-name string?)))

(s/def ::name-or-with-session-keyspace-pk-col-val-map
  (s/alt :table-pk-col (s/cat :table-name string? :pk-col-name-value-map map?)
         :keyspace-table-pk-col (s/cat :keyspace string? :table-name string? :pk-col-name-value-map map?)
         :session-keyspace-table-pk-col (s/cat :session session? :keyspace string? :table-name string? :pk-col-name-value-map map?)))

(s/def ::names-or-with-session-keyspace
  (s/alt :tables (s/cat :table-names vector?)
         :session-tables (s/cat :session session? :table-names vector?)
         :session-keyspace-tables (s/cat :session session? :keyspace string? :table-names vector?)))

(s/def ::two-names-or-with-session-keyspace
  (s/alt :two-tables (s/cat :base-table-name string? :query-table-name string?)
         :session-two-tables (s/cat :session session? :base-table-name string? :query-table-name string?)
         :session-keyspace-two-tables (s/cat :session session? :keyspace string? :base-table-name string? :query-table-name string?)))
