;; SPDX-License-Identifier: EPL-2.0

(ns jepsen.egres.db
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen.db :as db]
            [jepsen.egres.client :as client]
            [jepsen.store :as store])
  (:import (java.lang ProcessBuilder ProcessBuilder$Redirect)
           (java.net ServerSocket)
           (java.util.concurrent TimeUnit)))

(defn signal!
  "Send `sig` (STOP, CONT) to the server process, by pid. Java's Process
  API has no signals beyond destroy, so this shells out to kill(1)."
  [state sig]
  (when-let [^Process process (:process @state)]
    (let [command ["kill" (str "-" sig) (str (.pid process))]
          sender (-> (ProcessBuilder. ^java.util.List command) .inheritIO
                     .start)]
      (when-not (zero? (.waitFor sender))
        (throw (ex-info "kill failed" {:signal sig :pid (.pid process)}))))))

(defn stop! [state force?]
  (when-let [^Process process (:process @state)]
    (if force? (.destroyForcibly process) (.destroy process))
    (when-not (.waitFor process 15 TimeUnit/SECONDS)
      (.destroyForcibly process)
      (.waitFor process))
    (swap! state dissoc :process)))

(defn ready? [test log-file log-offset]
  (and (str/includes? (subs (slurp log-file) log-offset) "become-leader")
       (try
         (with-open [conn (client/connect! test)
                     stmt (.createStatement conn)
                     rows (.executeQuery stmt "SELECT 1")]
           (.next rows))
         (catch java.sql.SQLException _ false))))

(defn start! [state test]
  (when-not (some-> (:process @state) .isAlive)
    (let [dir (:directory @state)
          log-file (io/file dir "server.log")
          log-offset (if (.exists log-file) (count (slurp log-file)) 0)
          command [(:binary test)
                   "--raft=id=1"
                   (str "--listen=sql_port=" (:port test)
                        ";raft_port=" (inc (:port test))
                        ";client_port=" (+ 2 (:port test)))
                   (str "--store=dir_path=" (io/file dir "sst"))
                   (str "--wal=dir=" (io/file dir "wal"))
                   "--cache=total_bytes=536870912"]
          process (-> (ProcessBuilder. ^java.util.List command)
                      (.redirectErrorStream true)
                      (.redirectOutput
                        (ProcessBuilder$Redirect/appendTo log-file))
                      .start)]
      (swap! state assoc :process process)
      (loop [remaining 200]
        (when-not (.isAlive process)
          (throw (ex-info "Egres exited during startup"
                          {:log (str log-file) :exit (.exitValue process)})))
        (when-not (ready? test log-file log-offset)
          (when (zero? remaining)
            (throw (ex-info "Egres did not become ready"
                            {:log (str log-file)})))
          (Thread/sleep 100)
          (recur (dec remaining)))))))

(defrecord LocalDB [state]
  db/DB
  (setup! [this test node]
    ;; Refuse occupied ports so readiness cannot match an unrelated server.
    (doseq [port (range (:port test) (+ 3 (:port test)))]
      (with-open [socket (ServerSocket. port)]))
    (let [dir (.getCanonicalFile (store/path! test "server"))
          hook (Thread. ^Runnable #(stop! state true))]
      (.mkdirs dir)
      (reset! state {:directory dir :hook hook})
      (.addShutdownHook (Runtime/getRuntime) hook)
      (start! state test)))
  (teardown! [this test node]
    (stop! state false)
    (when-let [hook (:hook @state)]
      (.removeShutdownHook (Runtime/getRuntime) hook)))
  db/Process
  (start! [this test node] (start! state test))
  (kill! [this test node] (stop! state true))
  db/Pause
  (pause! [this test node] (signal! state "STOP"))
  (resume! [this test node] (signal! state "CONT")))
