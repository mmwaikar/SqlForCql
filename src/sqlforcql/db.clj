(ns sqlforcql.db
  (:gen-class)
  (:require [qbits.alia :as alia]
            [taoensso.timbre :refer [debug info]]))

(defn- get-db-map
  "Returns a map which contains a Cassandra cluster, session and keyspace information."
  ([session keyspace] (get-db-map nil session keyspace))

  ([cluster session keyspace]
   {:cluster cluster :session session :keyspace keyspace}))

(def default-db-map
  "An atom which is a map, which stores the Cassandra cluster, session and keyspace connected to."
  (atom (get-db-map nil nil "")))

(defn db-map-empty?
  "Determines if the default-db-map atom is empty or not."
  []
  (and (nil? (:session @default-db-map))
       (nil? (:keyspace @default-db-map))))

(defn get-empty-db-map []
  (get-db-map nil nil ""))

(defn connect-to-db
  "Connects to a Cassandra keyspace on a particular ip-address, with the given username and password."
  [ip-address username password keyspace]
  (let [cluster (alia/cluster {:contact-points [ip-address]
                               :credentials {:user username :password password}})
        session (alia/connect cluster)
        db-map (get-db-map cluster session keyspace)]
    (debug "Connecting to" keyspace "on" ip-address)
    (reset! default-db-map db-map)
    db-map))

(defn disconnect-from-db
  "Disconnects from Cassandra session and cluster information contained in the db-map."
  [db-map]
  (debug "Disconnecting... bye!")
  (alia/shutdown (:session db-map))
  (alia/shutdown (:cluster db-map))
  (reset! default-db-map (get-empty-db-map)))
