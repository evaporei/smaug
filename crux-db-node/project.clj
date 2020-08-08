(defproject crux-db-node "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "20.07-1.10.0-beta"]
                 [juxt/crux-http-server "20.07-1.10.0-alpha"]
                 [juxt/crux-rocksdb "20.08-1.10.1-beta"]
                 [juxt/crux-kafka "20.08-1.10.1-beta"]]
  :main ^:skip-aot crux-db-node.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
