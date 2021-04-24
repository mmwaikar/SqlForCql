(ns sqlforcql.analyze
  (:require [clojure.set :as s]
            [taoensso.timbre :refer [log debug info error]]
            [fipp.edn :refer [pprint] :rename {pprint fipp}]
            [sqlforcql.cql :as cql]))

(defn- get-name-count-map [name count]
  (if (keyword? name)
    {name count}
    {(keyword name) count}))

(defn get-counts
  "Get a count of number of rows in a table."
  ([table-names]
   (let [name-count-maps (map #(get-name-count-map % (cql/get-count %)) table-names)]
     (apply merge name-count-maps)))

  ([session table-names]
   (let [name-count-maps (map #(get-name-count-map % (cql/get-count session %)) table-names)]
     (apply merge name-count-maps)))

  ([session keyspace table-names]
   (let [name-count-maps (map #(get-name-count-map % (cql/get-count session keyspace %)) table-names)]
     (apply merge name-count-maps))))

(defn get-diff
  "Get the difference in number of rows in a main table vs. it's supporting query table."
  ([base-table-name query-table-name]
   (let [base-data (set (cql/get-all base-table-name))
         query-data (set (cql/get-all query-table-name))
         diff (cond
                (= (count base-data) (count query-data)) {:no-difference (set nil)}
                (> (count base-data) (count query-data)) {query-table-name (s/difference base-data query-data)}
                :else {base-table-name (s/difference query-data base-data)})]
     (fipp "difference:" diff)
     diff))

  ([session base-table-name query-table-name]
   (let [base-data (set (cql/get-all session base-table-name))
         query-data (set (cql/get-all session query-table-name))
         diff (cond
                (= (count base-data) (count query-data)) {:no-difference (set nil)}
                (> (count base-data) (count query-data)) {query-table-name (s/difference base-data query-data)}
                :else {base-table-name (s/difference query-data base-data)})]
     (fipp "difference:" diff)
     diff))

  ([session keyspace base-table-name query-table-name]
   (let [base-data (set (cql/get-all session keyspace base-table-name))
         query-data (set (cql/get-all session keyspace query-table-name))
         diff (cond
                (= (count base-data) (count query-data)) {:no-difference (set nil)}
                (> (count base-data) (count query-data)) {query-table-name (s/difference base-data query-data)}
                :else {base-table-name (s/difference query-data base-data)})]
     (fipp "difference:" diff)
     diff)))
