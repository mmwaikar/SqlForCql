(ns sqlforcql.schema
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [qbits.alia :as alia]
            [sqlforcql.db :as db]
            [sqlforcql.atoms :as atoms]
            [sqlforcql.core :as core]))

;; for keyspace
(defn- create-keyspace-stmt [keyspace]
  (str "CREATE KEYSPACE " keyspace " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};"))

(defn- create-keyspace [session keyspace]
  (alia/execute session (create-keyspace-stmt keyspace)))

(defn- use-keyspace-stmt [keyspace]
  (str "USE " keyspace ";"))

(defn- use-keyspace [session keyspace]
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
                         PRIMARY KEY (nickname));")

(def create-players-by-city-table-stmt
  "CREATE TABLE players (nickname varchar,
                         first_name varchar,
                         last_name varchar,
                         city varchar,
                         country varchar,
                         PRIMARY KEY (city, country));")

(defn- create-table-queries []
  [create-players-table-stmt create-players-by-city-table-stmt])

(defn- create-tables [session]
  (map #(alia/execute session %) (create-table-queries)))

;; for data
(defn- get-insert-map [nickname first_name last_name city country]
  {:nickname nickname
   :first_name first_name
   :last_name last_name
   :city city
   :country country})

(defn- insert-stmt [col-names-values-map]
  (let [col-names (keys col-names-values-map)
        col-values (map #(str "'" % "'") (vals col-names-values-map))
        col-str (str/join col-names)
        val-str (str/join col-values)]
    (str "INSERT INTO players (" col-str ") VALUES (" val-str ");")))

(defn- insert-data []
  (let [{session :session
         keyspace :keyspace} (deref atoms/default-db-map)
        fedex (get-insert-map "fedex" "Roger" "Federer" "Bern" "Switzerland")
        rafa (get-insert-map "rafa" "Rafael" "Nadal" "Madrid" "Spain")
        hariya (get-insert-map "hariya" "Hari" "Bhargava" "Ajmer" "India")
        manwa (get-insert-map "manwa" "Manoj" "Waikar" "Ajmer" "India")
        subu (get-insert-map "subu" "Subhashish" "Banerjee" "Abu Dhabi" "UAE")
        raju (get-insert-map "raju" "Ramchand" "Shahani" "Abu Dhabi" "UAE")
        data [fedex rafa hariya manwa subu raju]
        insert-stmts (map insert-stmt data)]
    (map #(alia/execute session %) insert-stmts)))

(defn- create-db
  "Let's generate a simple schema to test various queries."
  []
  (let [{session :session
         keyspace :keyspace} (deref atoms/default-db-map)]
    (create-keyspace session keyspace)
    (use-keyspace session keyspace)
    (create-tables session)))

(defn- destroy-db
  "Drop the keyspace to clean the DB."
  []
  (let [{session :session
         keyspace :keyspace} (deref atoms/default-db-map)]
    (alia/execute session (drop-keyspace-stmt keyspace))))

(defn db-test-fixture [f]
  (core/connect-to-default-db)
  (create-db)
  (insert-data)
  (f)
  (destroy-db)
  (core/disconnect-from-default-db))

(deftest generate-schema
  (testing "Should generate a simple schema to test various queries."
    (is (= 0 1))))
