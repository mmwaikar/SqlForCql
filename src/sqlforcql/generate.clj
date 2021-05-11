(ns sqlforcql.generate
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :refer [log debug info error]]
            [sqlforcql.specs :as specs])
  (:import (java.text SimpleDateFormat)
           (java.util Date UUID)
           (clojure.lang PersistentHashSet PersistentArrayMap)))

(def fmt (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss"))

(defn get-value [v]
  (let [v-type (type v)]
    (cond
      (= v-type String) (str "'" v "'")
      (= v-type Date) (str "'" (.format fmt v) "'")
      (= v-type UUID) v
      (= v-type PersistentHashSet) (subs (str v) 1)
      (= v-type PersistentArrayMap) (str/replace (json/write-str v) "\"" "'")
      :else (str "'" v "'"))))

(defn get-insert-statement
  "Generates an insert statement for a single row of a table (with name table-name). A row is represented as a set."
  [table-name row]
  {:pre [s/valid? ::specs/is-set row]}
  (let [col-names (map name (keys row))
        col-values (map #(get-value %) (vals row))
        col-str (str/join ", " col-names)
        val-str (str/join ", " col-values)]
    ;(debug "types: " (vec (map type (vals row))))
    (str "INSERT INTO " table-name "(" col-str ") VALUES (" val-str ");")))

(defn get-insert-statements
  "Generates multiple insert statements for rows of a table (with name table-name). Each row is represented as a set."
  [table-name rows]
  (let [insert-statements (map #(get-insert-statement table-name %) rows)]
    insert-statements))
