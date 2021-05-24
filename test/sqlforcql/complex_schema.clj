(ns sqlforcql.complex-schema
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [qbits.alia :as alia]
            [taoensso.timbre :refer [debug info]]
            [sqlforcql.atoms :as atoms]
            [sqlforcql.config :as config]
            [sqlforcql.core :as core]))

(comment
  (use 'sqlforcql.complex-schema)
  (clojure.test/run-tests 'sqlforcql.complex-schema)
  )

;; the below statement automatically wraps all the tests to connect
;; to the db, run tests and then disconnect from the db.
(use-fixtures :once config/db-test-fixture)

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
(def create-table-with-complex-columns
  "CREATE TABLE tbl_complex_cols (nickname varchar,
                                 city varchar,
                                 all_cities set<text>,
                                 schools map<text,text>,
                                 PRIMARY KEY (nickname));")

(def create-table-with-complex-columns-by-city
  "CREATE TABLE tbl_complex_cols_by_city (nickname varchar,
                                 city varchar,
                                 all_cities set<text>,
                                 schools map<text,text>,
                                 PRIMARY KEY (city));")

(defn- create-table-queries []
  [create-table-with-complex-columns])

(defn- create-tables [session]
  (doall
    (map #(alia/execute session %) (create-table-queries))))

;; for data
(defn- get-school-map [name city]
  {:name name :city city})

(defn- get-insert-map [nickname city cities schools]
  {:nickname   nickname
   :city       city
   :all_cities cities
   :schools    schools})

(defn- insert-stmt [table-name col-names-values-map]
  (let [col-names (map name (keys col-names-values-map))
        col-values (map #(str "'" % "'") (vals col-names-values-map))
        col-str (str/join ", " col-names)
        val-str (str/join ", " col-values)]
    (str "INSERT INTO " table-name " (" col-str ") VALUES (" val-str ");")))

;; https://cassandra.apache.org/doc/latest/cql/types.html

(defn- insert-data [table-name]
  (let [{session  :session
         keyspace :keyspace} (deref atoms/default-db-map)
        mannu (get-insert-map "mannu" "Pune" "{'Ajmer', 'Jodhpur', 'Meerut'}"
                              [(get-school-map "AMS" "Ajmer") (get-school-map "LMCST" "Jodhpur")])
        subu (get-insert-map "subu" "Dubai" "{'Ajmer', 'Jodhpur', 'Bangalore'}"
                             [(get-school-map "St. Anselm's" "Ajmer") (get-school-map "LMCST" "Jodhpur")])
        data [mannu subu]
        insert-stmts (map #(insert-stmt table-name %) data)]
    (debug (first insert-stmts))
    (doall
      (map #(alia/execute session %) insert-stmts))))

(defn- create-db
  "Let's generate a simple schema to test various queries."
  []
  (let [{session  :session
         keyspace :keyspace} (deref atoms/default-db-map)]
    ;(create-keyspace session keyspace)
    (use-keyspace session keyspace)
    ;(create-tables session)
    ))

(defn- destroy-db
  "Drop the keyspace to clean the DB."
  []
  (let [{session  :session
         keyspace :keyspace} (deref atoms/default-db-map)]
    (alia/execute session (drop-keyspace-stmt keyspace))))

(defn generate-schema []
  (info "Create a keyspace and some tables...")
  (create-db)

  (info "Insert data...")
  (insert-data "tbl_complex_cols")

  (info "Disconnect from the default DB...")
  (core/disconnect-from-default-db))

(deftest should_generate-schema
  (testing "Should generate a simple schema to test various queries."
    (generate-schema)
    (is (= 0 1))))
