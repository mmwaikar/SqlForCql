(ns sqlforcql.cql
  (:refer-clojure :exclude [update])
  (:require [clojure.string :as str]
            [qbits.alia :as alia]
            [qbits.hayt :refer [allow-filtering columns count* delete select set-columns update where]]
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

(defn get-count-query
  ([table-name] (select (keywordize-table-name table-name) (columns (count*))))
  ([keyspace table-name] (select (keywordize-table-name keyspace table-name) (columns (count*)))))

(defn get-by-pk-col-query
  ([table-name pk-col-name-value-map]
   (select (keywordize-table-name table-name) (where pk-col-name-value-map)))

  ([keyspace table-name pk-col-name-value-map]
   (select (keywordize-table-name keyspace table-name) (where pk-col-name-value-map))))

(defn get-by-non-pk-col-query
  ([table-name non-pk-col-name-value-map]
   (debug "use allow filtering clause")
   (select (keywordize-table-name table-name) (where non-pk-col-name-value-map) (allow-filtering)))

  ([keyspace table-name non-pk-col-name-value-map]
   (debug "use allow filtering clause")
   (select (keywordize-table-name keyspace table-name) (where non-pk-col-name-value-map) (allow-filtering))))

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

(defn get-count
  ([table-name]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (:count (first (alia/execute (:session @db-map) (get-count-query (:keyspace @db-map) table-name))))))

  ([session table-name]
   (:count (first (alia/execute session (get-count-query table-name)))))

  ([session keyspace table-name]
   (:count (first (alia/execute session (get-count-query keyspace table-name))))))

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
     (alia/execute (:session @db-map)
                   (get-by-non-pk-col-query (:keyspace @db-map) table-name non-pk-col-name-value-map))))

  ([session table-name non-pk-col-name-value-map]
   (alia/execute session (get-by-non-pk-col-query table-name non-pk-col-name-value-map)))

  ([session keyspace table-name non-pk-col-name-value-map]
   (alia/execute session (get-by-non-pk-col-query keyspace table-name non-pk-col-name-value-map))))

(defn- filter-using-like [rows non-pk-col-name-value-map]
  (let [col-name (first (keys non-pk-col-name-value-map))
        col-value (first (vals non-pk-col-name-value-map))]
    (filter #(str/includes? (% col-name) col-value) rows)))

(defn get-by-non-pk-col-like
  ([table-name non-pk-col-name-value-map]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (let [rows (get-all table-name)]
       (filter-using-like rows non-pk-col-name-value-map))))

  ([session table-name non-pk-col-name-value-map]
   (let [rows (get-all session table-name)]
     (filter-using-like rows non-pk-col-name-value-map)))

  ([session keyspace table-name non-pk-col-name-value-map]
   (let [rows (get-all session keyspace table-name)]
     (filter-using-like rows non-pk-col-name-value-map))))

(defn- update-query-where-single-col [keywordized-table-name rows pk-col-name update-map]
  (let [pk-col-values (map #(pk-col-name %) rows)
        pk-col-values-vec (apply vector pk-col-values)]
    (debug pk-col-values-vec)
    (update keywordized-table-name
            (set-columns update-map)
            (where [[:in pk-col-name pk-col-values-vec]]))))

(defn update-by-non-pk-col-query
  "This fn is used when there is only a partitioning key and no clustering columns.
  It has three overloads because it calls another function to get some data and
  forms update queries based on that data."
  ([table-name pk-col-name where-map update-map]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (let [rows (get-by-non-pk-col table-name where-map)
           keywordized-table-name (keywordize-table-name table-name)]
       (update-query-where-single-col keywordized-table-name rows pk-col-name update-map))))

  ([session table-name pk-col-name where-map update-map]
   (let [rows (get-by-non-pk-col session table-name where-map)
         keywordized-table-name (keywordize-table-name table-name)]
     (update-query-where-single-col keywordized-table-name rows pk-col-name update-map)))

  ([session keyspace table-name pk-col-name where-map update-map]
   (let [rows (get-by-non-pk-col session keyspace table-name where-map)
         keywordized-table-name (keywordize-table-name keyspace table-name)]
     (update-query-where-single-col keywordized-table-name rows pk-col-name update-map))))

(defn update-by-non-pk-col
  ([table-name pk-col-name where-map update-map]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (let [{session :session
            keyspace :keyspace} @db-map
           query (update-by-non-pk-col-query session keyspace table-name pk-col-name where-map update-map)]
       (info query)
       (alia/execute session query))))

  ([session table-name pk-col-name where-map update-map]
   (let [query (update-by-non-pk-col-query session table-name pk-col-name where-map update-map)]
     (info query)
     (alia/execute session query)))

  ([session keyspace table-name pk-col-name where-map update-map]
   (let [query (update-by-non-pk-col-query session keyspace table-name pk-col-name where-map update-map)]
     (info query)
     (alia/execute session query))))

;; (str/join " AND " (map (fn [[k v]] (str (name k) " = '" v "'")) m))
(defn- get-eq-where-cond [col-name-value-map]
  (map vector [= =] (keys col-name-value-map) (vals col-name-value-map)))

(defn- get-eq-where-conds [pk-clustering-col-maps]
  (map get-eq-where-cond pk-clustering-col-maps))

(defn- update-query-where-multiple-cols [keywordized-table-name rows pk-clustering-col-names update-map]
  (let [pk-clustering-col-maps (map #(select-keys % pk-clustering-col-names) rows)
        where-vecs (get-eq-where-conds pk-clustering-col-maps)
        queries (map #(update keywordized-table-name
                              (set-columns update-map)
                              (where %)) where-vecs)]
    (debug (first queries))
    queries))

(defn update-by-non-pk-col-with-clustering-col-query
  "This fn is used when there is a partitioning key and one or more clustering columns.
  It has three overloads because it calls another function to get some data and
  forms update queries based on that data."
  ([table-name pk-clustering-col-names where-map update-map]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (let [rows (get-by-non-pk-col table-name where-map)
           keywordized-table-name (keywordize-table-name table-name)]
       (update-query-where-multiple-cols keywordized-table-name rows pk-clustering-col-names update-map))))

  ([session table-name pk-clustering-col-names where-map update-map]
   (let [rows (get-by-non-pk-col session table-name where-map)
         keywordized-table-name (keywordize-table-name table-name)]
     (update-query-where-multiple-cols keywordized-table-name rows pk-clustering-col-names update-map)))

  ([session keyspace table-name pk-clustering-col-names where-map update-map]
   (let [rows (get-by-non-pk-col session keyspace table-name where-map)
         keywordized-table-name (keywordize-table-name keyspace table-name)]
     (update-query-where-multiple-cols keywordized-table-name rows pk-clustering-col-names update-map))))

(defn update-by-non-pk-col-with-clustering-col
  ([table-name pk-clustering-col-names where-map update-map]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (let [{session :session
            keyspace :keyspace} @db-map
           queries (update-by-non-pk-col-with-clustering-col-query session keyspace table-name pk-clustering-col-names
                                                                   where-map update-map)]
       (doall
         (map #(alia/execute session %) queries)))))

  ([session table-name pk-clustering-col-names where-map update-map]
   (let [queries (update-by-non-pk-col-with-clustering-col-query session table-name pk-clustering-col-names
                                                                 where-map update-map)]
     (doall
       (map #(alia/execute session %) queries))))

  ([session keyspace table-name pk-clustering-col-names where-map update-map]
   (let [queries (update-by-non-pk-col-with-clustering-col-query session keyspace table-name pk-clustering-col-names
                                                                 where-map update-map)]
     (doall
       (map #(alia/execute session %) queries)))))

(defn- contains-element? [coll e]
  (some #(= e %) coll))

(defn delete-multiple-by-pk-col
  ([table-name pk-col-name values-to-exclude]
   (if (db-map-empty?)
     (do
       (error "Set session and keyspace (to avoid specifying it in every fn call) by using the set-db-map! fn.")
       (into {} []))
     (let [{session :session
            keyspace :keyspace} @db-map
           rows (get-all session table-name)
           pk-col-values (map #(pk-col-name %) rows)
           excluding-key-values-vec (vector (remove #(contains-element? values-to-exclude %) pk-col-values))
           delete-queries (map #(delete (keywordize-table-name table-name)
                                        (where [[:in pk-col-name excluding-key-values-vec]])))]
       (debug (first delete-queries))
       (doall
         (map #(alia/execute session %) delete-queries)))))

  ([session table-name pk-col-name values-to-exclude]
   (let [rows (get-all session table-name)
         pk-col-values (map #(pk-col-name %) rows)
         excluding-key-values-vec (vector (remove #(contains-element? values-to-exclude %) pk-col-values))
         delete-queries (map #(delete (keywordize-table-name table-name)
                                      (where [[:in pk-col-name excluding-key-values-vec]])))]
     (debug (first delete-queries))
     (doall
       (map #(alia/execute session %) delete-queries))))

  ([session keyspace table-name pk-col-name values-to-exclude]
   (let [rows (get-all session table-name)
         pk-col-values (map #(pk-col-name %) rows)
         excluding-key-values-vec (vector (remove #(contains-element? values-to-exclude %) pk-col-values))
         delete-queries (map #(delete (keywordize-table-name keyspace table-name)
                                      (where [[:in pk-col-name excluding-key-values-vec]])))]
     (debug (first delete-queries))
     (doall
       (map #(alia/execute session %) delete-queries)))))
