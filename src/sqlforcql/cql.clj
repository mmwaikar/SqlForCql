(ns sqlforcql.cql
  (:refer-clojure :exclude [update])
  (:require [clojure.string :as str]
            [qbits.alia :as alia]
            [qbits.hayt :refer [allow-filtering select set-columns update where]]
            [taoensso.timbre :refer [log debug info error]]
            [sqlforcql.db :as db]))

(def db-map (atom (db/get-db-map nil nil nil)))

(defn db-map-empty? []
  (if (and (nil? (:session @db-map))
           (nil? (:keyspace @db-map)))
    true
    false))

(defn set-db-map! [session keyspace]
  (reset! db-map (db/get-db-map session keyspace)))

(defn keywordize-table-name
  ([table-name] (keyword table-name))
  ([keyspace table-name] (keyword (str keyspace "." table-name))))

;; query related functions
(defn get-all-query
  ([table-name] (select (keywordize-table-name table-name)))
  ([keyspace table-name] (select (keywordize-table-name keyspace table-name))))

(defn get-by-pk-col-query
  ([table-name pk-col-name-value-map]
   (select (keywordize-table-name table-name) (where pk-col-name-value-map)))

  ([keyspace table-name pk-col-name-value-map]
   (select (keywordize-table-name keyspace table-name) (where pk-col-name-value-map))))

(defn get-by-non-pk-col-query
  ([table-name non-pk-col-name-value-map]
   (select (keywordize-table-name table-name) (where non-pk-col-name-value-map) (allow-filtering)))

  ([keyspace table-name non-pk-col-name-value-map]
   (select (keywordize-table-name keyspace table-name) (where non-pk-col-name-value-map) (allow-filtering))))

(defn update-by-non-pk-col-query
  ([table-name to-update-cols-name-values-map non-pk-col-name-value-map]
   (let [rows (get-by-non-pk-col-query table-name)])
   (update (keywordize-table-name table-name)
           (set-columns to-update-cols-name-values-map)
           (where non-pk-col-name-value-map)))

  ([keyspace table-name to-update-cols-name-values-map non-pk-col-name-value-map]
   (update (keywordize-table-name keyspace table-name)
           (set-columns to-update-cols-name-values-map)
           (where non-pk-col-name-value-map))))

;; functions related to executing queries
(defn get-all
  ([table-name]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (alia/execute (:session @db-map) (get-all-query (:keyspace @db-map) table-name))))

  ([session table-name]
   (alia/execute session (get-all-query table-name)))

  ([session keyspace table-name]
   (alia/execute session (get-all-query keyspace table-name))))

(defn get-by-pk-col
  ([table-name pk-col-name-value-map]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (alia/execute (:session @db-map) (get-by-pk-col-query (:keyspace @db-map) table-name pk-col-name-value-map))))

  ([session table-name pk-col-name-value-map]
   (alia/execute session (get-by-pk-col-query table-name pk-col-name-value-map)))

  ([session keyspace table-name pk-col-name-value-map]
   (alia/execute session (get-by-pk-col-query keyspace table-name pk-col-name-value-map))))

(defn get-by-non-pk-col
  ([table-name non-pk-col-name-value-map]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (alia/execute (:session @db-map) (get-by-non-pk-col-query (:keyspace @db-map) table-name non-pk-col-name-value-map))))

  ([session table-name non-pk-col-name-value-map]
   (alia/execute session (get-by-non-pk-col-query table-name non-pk-col-name-value-map)))

  ([session keyspace table-name non-pk-col-name-value-map]
   (alia/execute session (get-by-non-pk-col-query keyspace table-name non-pk-col-name-value-map))))

(defn get-by-non-pk-col-like
  ([table-name non-pk-col-name-value-map]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (let [rows (get-all table-name)
           col-name (first (keys non-pk-col-name-value-map))
           col-value (first (vals non-pk-col-name-value-map))]
       (filter #(str/includes? (% col-name) col-value) rows))))

  ([session table-name non-pk-col-name-value-map]
   (let [rows (get-all session table-name)
         col-name (first (keys non-pk-col-name-value-map))
         col-value (first (vals non-pk-col-name-value-map))]
     (filter #(str/includes? (non-pk-col-name-value-map col-name) col-value) rows)))

  ([session keyspace table-name non-pk-col-name-value-map]
   (let [rows (get-all session keyspace table-name)
         col-name (first (keys non-pk-col-name-value-map))
         col-value (first (vals non-pk-col-name-value-map))]
     (filter #(str/includes? (% col-name) col-value) rows))))

(defn update-by-non-pk-col-query
  "This fn is used when there is only a partitioning key and no clustering columns."
  ([table-name pk-col-name where-map update-map]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (let [rows (get-by-non-pk-col table-name where-map)
           pk-col-values (map #(pk-col-name %) rows)
           pk-col-values-vec (apply vector pk-col-values)]
       (debug pk-col-values-vec)
       (update (keywordize-table-name table-name)
               (set-columns update-map)
               (where [[:in pk-col-name pk-col-values-vec]])))))

  ([keyspace table-name pk-col-name where-map update-map]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (let [rows (get-by-non-pk-col table-name where-map)
           pk-col-values (map #(pk-col-name %) rows)
           pk-col-values-vec (apply vector pk-col-values)]
       (debug pk-col-values-vec)
       (update (keywordize-table-name keyspace table-name)
               (set-columns update-map)
               (where [[:in pk-col-name pk-col-values-vec]]))))))

(defn- get-eq-where-cond [col-name]
  `[= ~col-name (~col-name %)])

(defn- get-eq-where-conds [pk-clustering-col-names]
  (map get-eq-where-cond pk-clustering-col-names))

(defn update-by-non-pk-col-with-clustering-col-query
  "This fn is used when there is a partitioning key and one or more clustering columns."
  ([table-name pk-clustering-col-names where-map update-map]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (let [rows (get-by-non-pk-col table-name where-map)
           pk-clustering-col-maps (map #(select-keys % pk-clustering-col-names) rows)
           where-vecs (get-eq-where-conds pk-clustering-col-names)
           queries (map #(update (keywordize-table-name table-name)
                                 (set-columns update-map)
                                 (where [where-vecs])) pk-clustering-col-maps)]
       (debug (first queries))
       queries)))

  ([keyspace table-name pk-clustering-col-names where-map update-map]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (let [rows (get-by-non-pk-col table-name where-map)
           pk-clustering-col-maps (map #(select-keys % pk-clustering-col-names) rows)
           where-vecs (get-eq-where-conds pk-clustering-col-names)
           queries (map #(update (keywordize-table-name keyspace table-name)
                                 (set-columns update-map)
                                 (where [where-vecs])) pk-clustering-col-maps)]
       (debug (first queries))
       queries))))

(defn update-by-non-pk-col
  ([table-name pk-col-name where-map update-map]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (let [{session :session
            keyspace :keyspace} @db-map
           query (update-by-non-pk-col-query keyspace table-name pk-col-name where-map update-map)]
       (info query)
       (alia/execute session query))))

  ([session table-name pk-col-name where-map update-map]
   (let [query (update-by-non-pk-col-query table-name pk-col-name where-map update-map)]
     (info query)
     (alia/execute session query)))

  ([session keyspace table-name pk-col-name where-map update-map]
   (let [query (update-by-non-pk-col-query keyspace table-name pk-col-name where-map update-map)]
     (info query)
     (alia/execute session query))))
