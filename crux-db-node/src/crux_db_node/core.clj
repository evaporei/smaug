(ns crux-db-node.core
  (:gen-class)
  (:require [crux.api :as crux]
            [clojure.java.io :as io]))

(defn start-standalone-http-node [port storage-dir]
  (crux/start-node {:crux.node/topology '[crux.standalone/topology crux.http-server/module]
                    :crux.standalone/event-log-dir (io/file storage-dir "event-log")
                    :crux.kv/db-dir (io/file storage-dir "indexes")
                    :crux.http-server/port port}))

(defn -main
  [& args]
  (println "Starting CruxDB at 3000 port")
  (start-standalone-http-node 3000 "store"))
