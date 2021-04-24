(ns sqlforcql.querybuilder
  (:require [qbits.hayt :refer [allow-filtering columns count* select where]]
            [taoensso.timbre :refer [debug]]
            [clojure.spec.alpha :as s]
            [sqlforcql.specs :as specs]))

(defn keywordize-table-name
  "Converts the name of the table `table-name` into a keyword. If the name
  of the keyspace is also specified, then it converts `keyspace.table-name`
  into a keyword."
  ([table-name]
   {:pre [(s/valid? ::specs/name-or-with-keyspace [table-name])]}
   (keyword table-name))

  ([keyspace table-name]
   {:pre [(s/valid? ::specs/name-or-with-keyspace [keyspace table-name])]}
   (keyword (str keyspace "." table-name))))

(defn get-all-query
  "Constructs a `select * from table-name` query."
  ([table-name]
   {:pre [(s/valid? ::specs/name-or-with-keyspace [table-name])]}
   (select (keywordize-table-name table-name)))

  ([keyspace table-name]
   {:pre [(s/valid? ::specs/name-or-with-keyspace [keyspace table-name])]}
   (select (keywordize-table-name keyspace table-name))))

(defn get-count-query
  "Constructs a `select count(*) from table-name` query."
  ([table-name]
   {:pre [(s/valid? ::specs/name-or-with-keyspace [table-name])]}
   (select (keywordize-table-name table-name) (columns (count*))))

  ([keyspace table-name]
   {:pre [(s/valid? ::specs/name-or-with-keyspace [keyspace table-name])]}
   (select (keywordize-table-name keyspace table-name) (columns (count*)))))

(defn get-by-pk-col-query
  "Constructs a `select * from table-name where pk-col-name = value` query."
  ([table-name pk-col-name-value-map]
   {:pre [(s/valid? ::specs/name-or-with-session-keyspace-pk-col-val-map [table-name pk-col-name-value-map])]}
   (select (keywordize-table-name table-name) (where pk-col-name-value-map)))

  ([keyspace table-name pk-col-name-value-map]
   {:pre [(s/valid? ::specs/name-or-with-session-keyspace-pk-col-val-map [keyspace table-name pk-col-name-value-map])]}
   (select (keywordize-table-name keyspace table-name) (where pk-col-name-value-map))))

(defn get-by-non-pk-col-query
  "Constructs a `select * from table-name where non-pk-col = value allow filtering` query."
  ([table-name non-pk-col-name-value-map]
   {:pre [(s/valid? ::specs/name-or-with-session-keyspace-pk-col-val-map [table-name non-pk-col-name-value-map])]}
   (debug "use allow filtering clause")
   (select (keywordize-table-name table-name) (where non-pk-col-name-value-map) (allow-filtering)))

  ([keyspace table-name non-pk-col-name-value-map]
   {:pre [(s/valid? ::specs/name-or-with-session-keyspace-pk-col-val-map [keyspace table-name non-pk-col-name-value-map])]}
   (debug "use allow filtering clause")
   (select (keywordize-table-name keyspace table-name) (where non-pk-col-name-value-map) (allow-filtering))))
