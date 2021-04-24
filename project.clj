(defproject sqlforcql "0.2.1"
  :description "SQL like capabilities for Apache Cassandra."
  :url "https://github.com/mmwaikar/SqlForCql/tree/develop"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [cc.qbits/alia-all "4.3.3"]
                 [cc.qbits/hayt "4.1.0"]
                 [com.taoensso/timbre "4.10.0"]
                 [fipp "0.6.23"]]
  :repl-options {:init-ns sqlforcql.core}
  :profiles {:dev
             {:dependencies [[org.clojure/test.check "1.1.0"]]}})
