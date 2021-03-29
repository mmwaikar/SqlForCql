(ns sqlforcql.analyze
  (:require [taoensso.timbre :refer [log debug info error]]
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
