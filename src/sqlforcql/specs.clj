(ns sqlforcql.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]))

(s/def ::keyspace string?)
(s/def ::table-name string?)
(s/def ::pk-col-name-value-map map?)

(s/def ::seq-with-single-val (s/and seq? #(= (count %) 1)))

(s/def ::name-or-with-keyspace
  (s/alt :table (s/cat :table-name string?)
         :keyspace-table (s/cat :keyspace string? :table-name string?)))

(s/def ::name-or-with-session-keyspace
  (s/alt :table (s/cat :table-name string?)
         :session-table (s/cat :session string? :table-name string?)
         :session-keyspace-table (s/cat :session string? :keyspace string? :table-name string?)))

(s/def ::name-or-with-session-keyspace-pk-col-val-map
  (s/alt :table-pk-col (s/cat :table-name string? :pk-col-name-value-map map?)
         :keyspace-table-pk-col (s/cat :keyspace string? :table-name string? :pk-col-name-value-map map?)
         :session-keyspace-table-pk-col (s/cat :session string? :keyspace string? :table-name string? :pk-col-name-value-map map?)))
