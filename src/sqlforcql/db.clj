(ns sqlforcql.db
  (:require [qbits.alia :as alia]
            [taoensso.timbre :refer [debug info]]))

(defn get-db-map
  ([session keyspace] (get-db-map nil session keyspace))

  ([cluster session keyspace]
   {:cluster cluster :session session :keyspace keyspace}))

(defn connect-to-db [ip-address username password keyspace]
  (let [cluster (alia/cluster {:contact-points [ip-address]
                               :credentials {:user username :password password}})
        session (alia/connect cluster)]
    (debug "Connecting to" keyspace "on" ip-address)
    (get-db-map cluster session keyspace)))

(defn disconnect-from-db [db-map]
  (debug "Disconnecting... bye!")
  (alia/shutdown (:session db-map))
  (alia/shutdown (:cluster db-map)))
