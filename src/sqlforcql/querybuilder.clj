(ns sqlforcql.querybuilder
  (:require [qbits.hayt :refer [allow-filtering columns count* select where]]
            [taoensso.timbre :refer [debug]]))

(defn keywordize-table-name
  ([table-name] (keyword table-name))
  ([keyspace table-name] (keyword (str keyspace "." table-name))))

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
