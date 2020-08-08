(ns crux-db-node.core
  (:gen-class)
  (:require [crux.api :as crux]
            [clojure.java.io :as io]))

(defn start-node [http-port kafka-port storage-dir]
  (crux/start-node {:crux.node/topology '[crux.standalone/topology crux.kv.rocksdb/kv-store crux.http-server/module]
                    :crux.standalone/event-log-dir (io/file storage-dir "event-log")
                    :crux.standalone/event-log-kv-store 'crux.kv.rocksdb/kv
                    :crux.kafka/bootstrap-servers (str "localhost:" kafka-port)
                    :crux.kv/db-dir (io/file storage-dir "indexes")
                    :crux.http-server/port http-port}))

(defn -main
  [& args]
  (println "Starting CruxDB, with HTTP server at 3000 port")
  (start-node 3000 9092 "store"))
