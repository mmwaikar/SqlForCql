(ns sqlforcql.generate
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :refer [log debug info error]]
            [sqlforcql.querybuilder :as qb]
            [sqlforcql.specs :as specs])
  (:import (java.text SimpleDateFormat)
           (java.util Date UUID)
           (clojure.lang PersistentHashSet PersistentArrayMap)))

(def single-quote "'")
(def double-quote "\"")
(def two-single-quotes "''")
(def fmt (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss"))

(defn- wrap-in-single-quotes
  "Wraps a value v in single-quotes, resulting in 'v'."
  [v]
  (str single-quote v single-quote))

(defn- escape-single-quotes
  "Replaces a value containing single-quotes to one with two single-quotes, i.e. escapes a single quote.
  So a value like Part('abcd') results in Part(''abcd'')"
  [v]
  (str/replace v single-quote two-single-quotes))

(defn- replace-double-with-single-quotes
  "Replaces a value \"v\" with double-quotes to one with single-quotes, resulting in 'v'."
  [v]
  (str/replace v double-quote single-quote))

(defn- set-to-str
  "Converts a set of values to a string, which can be inserted in a set<text> column in Cassandra."
  [a-set]
  (let [values-str (str/join ", " (map #(wrap-in-single-quotes (escape-single-quotes %)) a-set))]
    (str "{" values-str "}")))

(defn- map-to-str
  "Converts a map to a string, which can be inserted in a map<text,text> column in Cassandra."
  [a-map]
  (let [kv-strings (for [[k v] a-map]
                     (str (wrap-in-single-quotes k) ":" (wrap-in-single-quotes (escape-single-quotes v))))
        comma-separated-kv-str (str/join ", " kv-strings)]
    (str "{" comma-separated-kv-str "}")))

(defn get-value [v]
  (let [v-type (type v)]
    (cond
      (= v-type String) (wrap-in-single-quotes (escape-single-quotes v))
      (= v-type Date) (str "'" (.format fmt v) "'")
      (= v-type UUID) v
      (= v-type Boolean) v
      (= v-type PersistentHashSet) (set-to-str v)
      (= v-type PersistentArrayMap) (map-to-str v)
      :else (wrap-in-single-quotes (escape-single-quotes v)))))

(defn get-insert-statement
  "Generates an insert statement for a single row of a table (with name table-name). A row is represented as a set."
  [keyspace table-name row]
  {:pre [s/valid? ::specs/is-set row]}
  (let [col-names (map name (keys row))
        col-values (map #(get-value %) (vals row))
        col-str (str/join ", " col-names)
        val-str (str/join ", " col-values)]
    ;(debug "types: " (vec (map type (vals row))))
    (str "INSERT INTO " (qb/get-table-name keyspace table-name) "(" col-str ") VALUES (" val-str ");")))

(defn get-insert-statements
  "Generates multiple insert statements for rows of a table (with name table-name). Each row is represented as a set."
  [keyspace table-name rows]
  (let [insert-statements (map #(get-insert-statement keyspace table-name %) rows)]
    insert-statements))
