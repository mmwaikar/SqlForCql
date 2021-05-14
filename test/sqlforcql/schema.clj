(ns sqlforcql.schema
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [qbits.alia :as alia]
            [taoensso.timbre :refer [debug info]]
            [sqlforcql.db :as db]
            [sqlforcql.core :as core]))

;; for keyspace
(defn- create-keyspace-stmt [keyspace]
  (str "CREATE KEYSPACE " keyspace " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};"))

(defn- create-keyspace [session keyspace]
  (info "Creating a keyspace...")
  (alia/execute session (create-keyspace-stmt keyspace)))

(defn- use-keyspace-stmt [keyspace]
  (str "USE " keyspace ";"))

(defn- use-keyspace [session keyspace]
  (info "Switching to keyspace...")
  (alia/execute session (use-keyspace-stmt keyspace)))

(defn- drop-keyspace-stmt [keyspace]
  (str "DROP " keyspace ";"))

;; for tables
(def create-players-table-stmt
  "CREATE TABLE players (nickname varchar,
                         first_name varchar,
                         last_name varchar,
                         city varchar,
                         country varchar,
                         zip varchar,
                         PRIMARY KEY (nickname));")

(def create-players-by-city-table-stmt
  "CREATE TABLE players_by_city (nickname varchar,
                                 first_name varchar,
                                 last_name varchar,
                                 city varchar,
                                 country varchar,
                                 zip varchar,
                                 PRIMARY KEY (city, country));")

(defn- create-table-queries []
  [create-players-table-stmt create-players-by-city-table-stmt])

(defn- create-tables [session]
  (doall
    (map #(alia/execute session %) (create-table-queries))))

;; for data
(defn- get-insert-map [nickname first_name last_name city country zip]
  {:nickname nickname
   :first_name first_name
   :last_name last_name
   :city city
   :country country
   :zip zip})

(defn- insert-stmt [table-name col-names-values-map]
  (let [col-names (map name (keys col-names-values-map))
        col-values (map #(str "'" % "'") (vals col-names-values-map))
        col-str (str/join ", " col-names)
        val-str (str/join ", " col-values)]
    (str "INSERT INTO " table-name " (" col-str ") VALUES (" val-str ");")))

(defn- insert-data [table-name]
  (let [{session :session
         keyspace :keyspace} @db/default-db-map
        fedex (get-insert-map "fedex" "Roger" "Federer" "Bern" "Switzerland" "3001")
        rafa (get-insert-map "rafa" "Rafael" "Nadal" "Madrid" "Spain" "28001")
        naseer (get-insert-map "naseer" "Naseeruddin" "Shah" "Ajmer" "India" "305001")
        chintu (get-insert-map "chintu" "Rishi" "Kapoor" "Jodhpur" "India" "305001")
        sonu (get-insert-map "sonu" "Sonu" "Nigam" "Dubai" "UAE" "00000")
        king (get-insert-map "king" "Shahrukh" "Khan" "Abu Dhabi" "UAE" "00000")
        data [fedex rafa naseer chintu sonu king]
        insert-stmts (map #(insert-stmt table-name %) data)]
    (debug (first insert-stmts))
    (doall
      (map #(alia/execute session %) insert-stmts))))

(defn- create-db
  "Let's generate a simple schema to test various queries."
  []
  (let [{session :session
         keyspace :keyspace} @db/default-db-map]
    (create-keyspace session keyspace)
    (use-keyspace session keyspace)
    (create-tables session)))

(defn- destroy-db
  "Drop the keyspace to clean the DB."
  []
  (let [{session :session
         keyspace :keyspace} @db/default-db-map]
    (alia/execute session (drop-keyspace-stmt keyspace))))

(defn generate-schema []
  (info "Connect to the default DB...")
  (core/connect-to-default-db "localhost" "" "" "sqlforcql")

  (info "Create a keyspace and some tables...")
  (create-db)

  (info "Insert data...")
  (insert-data "players")
  (insert-data "players_by_city")

  (info "Disconnect from the default DB...")
  (core/disconnect-from-default-db))

(deftest should_generate-schema
  (testing "Should generate a simple schema to test various queries."
    (generate-schema)
    (is (= 0 1))))
