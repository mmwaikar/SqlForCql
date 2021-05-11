(ns sqlforcql.analyze
  (:gen-class)
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :refer [log debug info error]]
            [fipp.edn :refer [pprint] :rename {pprint fipp}]
            [sqlforcql.cql :as cql]
            [sqlforcql.specs :as specs]))

(defn- get-name-count-map [name count]
  (if (keyword? name)
    {name count}
    {(keyword name) count}))

(defn get-counts
  "Get a count of number of rows in a table."
  ([table-names]
   {:pre [(s/valid? ::specs/names-or-with-session-keyspace [table-names])]
    :post [(s/valid? map? %)]}
   (let [name-count-maps (map #(get-name-count-map % (cql/get-count %)) table-names)]
     (apply merge name-count-maps)))

  ([session table-names]
   {:pre [(s/valid? ::specs/names-or-with-session-keyspace [session table-names])]
    :post [(s/valid? map? %)]}
   (let [name-count-maps (map #(get-name-count-map % (cql/get-count session %)) table-names)]
     (apply merge name-count-maps)))

  ([session keyspace table-names]
   {:pre [(s/valid? ::specs/names-or-with-session-keyspace [session keyspace table-names])]
    :post [(s/valid? map? %)]}
   (let [name-count-maps (map #(get-name-count-map % (cql/get-count session keyspace %)) table-names)]
     (apply merge name-count-maps))))

(defn get-diff
  "Get the difference in number of rows in a base table vs. it's supporting query table.
  If the number of rows are the same, {:no-difference #{}} is returned. Else, a map is
  returned with table name as key and value as a set of rows to be inserted in the table."
  ([base-table-name query-table-name]
   {:pre [(s/valid? ::specs/two-names-or-with-session-keyspace [base-table-name query-table-name])]
    :post [(s/valid? map? %)]}
   (let [base-data (set (cql/get-all base-table-name))
         query-data (set (cql/get-all query-table-name))
         diff (cond
                (= (count base-data) (count query-data)) {:no-difference (set nil)}
                (> (count base-data) (count query-data)) {query-table-name (set/difference base-data query-data)}
                :else {base-table-name (set/difference query-data base-data)})]
     ;(fipp "difference:" diff)
     diff))

  ([session base-table-name query-table-name]
   {:pre [(s/valid? ::specs/two-names-or-with-session-keyspace [session base-table-name query-table-name])]
    :post [(s/valid? map? %)]}
   (let [base-data (set (cql/get-all session base-table-name))
         query-data (set (cql/get-all session query-table-name))
         diff (cond
                (= (count base-data) (count query-data)) {:no-difference (set nil)}
                (> (count base-data) (count query-data)) {query-table-name (set/difference base-data query-data)}
                :else {base-table-name (set/difference query-data base-data)})]
     (fipp "difference:" diff)
     diff))

  ([session keyspace base-table-name query-table-name]
   {:pre [(s/valid? ::specs/two-names-or-with-session-keyspace [session keyspace base-table-name query-table-name])]
    :post [(s/valid? map? %)]}
   (let [base-data (set (cql/get-all session keyspace base-table-name))
         query-data (set (cql/get-all session keyspace query-table-name))
         diff (cond
                (= (count base-data) (count query-data)) {:no-difference (set nil)}
                (> (count base-data) (count query-data)) {query-table-name (set/difference base-data query-data)}
                :else {base-table-name (set/difference query-data base-data)})]
     (fipp "difference:" diff)
     diff)))
