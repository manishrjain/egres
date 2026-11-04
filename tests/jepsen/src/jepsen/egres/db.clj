;; SPDX-License-Identifier: EPL-2.0

(ns jepsen.egres.db
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen.db :as db]
            [jepsen.egres.client :as client]
            [jepsen.store :as store]
            [next.jdbc :as j])
  (:import (java.io File RandomAccessFile)
           (java.lang ProcessBuilder ProcessBuilder$Redirect)
           (java.net ServerSocket)
           (java.nio.file Files Path)
           (java.util.concurrent TimeUnit)))

(defn exec!
  "Run `command` to completion on the harness's stdio; throws when it
  exits nonzero."
  [& command]
  (let [process (-> (ProcessBuilder. ^java.util.List (vec command))
                    .inheritIO .start)]
    (when-not (zero? (.waitFor process))
      (throw (ex-info "command failed" {:command command
                                        :exit (.exitValue process)})))))

(defn signal!
  "Send `sig` (STOP, CONT) to the server process, by pid. Java's Process
  API has no signals beyond destroy, so this shells out to kill(1)."
  [state sig]
  (when-let [^Process process (:process @state)]
    (exec! "kill" (str "-" sig) (str (.pid process)))))

(defn stop! [state force?]
  (when-let [^Process process (:process @state)]
    (if force? (.destroyForcibly process) (.destroy process))
    (when-not (.waitFor process 15 TimeUnit/SECONDS)
      (.destroyForcibly process)
      (.waitFor process))
    (swap! state dissoc :process)))

;; lazyfs (github.com/dsrhaslab/lazyfs) is a FUSE filesystem with its
;; own page cache: a write reaches the backing directory on fsync, or
;; never, when the cache is cleared first. With --lazyfs the server's
;; data lives on it and each kill clears the cache, so a write the
;; kernel's page cache would have kept is lost, as a power cut loses
;; it. The server's log stays on the real filesystem.

(defn lazyfs-paths
  "The files of a lazyfs under the test's server directory: the mount
  the server writes to, the backing directory that keeps what was
  synced, the command FIFO and the one that answers a clear, the
  config and the two logs."
  [dir]
  (let [base (io/file dir "lazyfs")]
    {:mount (io/file base "mnt")
     :root (io/file base "root")
     :fifo (io/file base "fifo")
     :completed (io/file base "completed")
     :config (io/file base "config.toml")
     :log (io/file base "lazyfs.log")
     :daemon-log (io/file base "daemon.log")}))

(defn lazyfs-config
  "lazyfs's TOML. The cache never evicts on its own, so only a clear
  loses data; its size caps the unsynced bytes between two syncs."
  [{:keys [fifo completed log]}]
  (str "[faults]\n"
       "fifo_path=\"" fifo "\"\n"
       "fifo_path_completed=\"" completed "\"\n"
       "[cache]\n"
       "apply_eviction=false\n"
       "[cache.simple]\n"
       "custom_size=\"1GB\"\n"
       "blocks_per_page=1\n"
       "[filesystem]\n"
       "logfile=\"" log "\"\n"
       "log_all_operations=false\n"))

(defn mounts
  "The text of /proc/self/mounts. Read whole: a procfs file has no
  size, which the reader behind slurp asks for."
  []
  (String. (Files/readAllBytes (Path/of "/proc/self/mounts"
                                        (make-array String 0)))))

(defn mounted?
  "Whether `mounts`, the text of /proc/self/mounts, lists a lazyfs at
  `mount`."
  [mounts ^File mount]
  (let [path (.getPath mount)]
    (boolean (some (fn [line]
                     (let [[_ where type] (str/split line #" ")]
                       (and (= where path) (= type "fuse.lazyfs"))))
                   (str/split-lines mounts)))))

(defn mount-lazyfs! [state test]
  (let [{:keys [mount root fifo completed config log daemon-log]
         :as paths} (lazyfs-paths (:directory @state))]
    (.mkdirs mount)
    (.mkdirs root)
    (spit config (lazyfs-config paths))
    (spit log "")
    (exec! "mkfifo" (str fifo) (str completed))
    ;; lazyfs opens the answer FIFO for writing at startup, which
    ;; blocks until a reader opens; read-write here opens at once and
    ;; stays open to read each answer.
    ;; Without -f libfuse forks after the mount and the process started
    ;; here exits, so the daemon is only reachable through the mount and
    ;; the liveness check below races the fork.
    (let [answers (RandomAccessFile. ^File completed "rw")
          command [(:lazyfs-binary test) "-f" (str mount)
                   "--config-path" (str config)
                   "-o" "modules=subdir" "-o" (str "subdir=" root)]
          process (-> (ProcessBuilder. ^java.util.List command)
                      (.redirectErrorStream true)
                      (.redirectOutput
                        (ProcessBuilder$Redirect/appendTo daemon-log))
                      .start)]
      (swap! state assoc :lazyfs {:process process :answers answers
                                  :paths paths})
      (loop [remaining 100]
        (when-not (.isAlive process)
          (throw (ex-info "lazyfs exited during startup"
                          {:log (str daemon-log)
                           :exit (.exitValue process)})))
        (when-not (mounted? (mounts) mount)
          (when (zero? remaining)
            (throw (ex-info "lazyfs did not mount" {:log (str daemon-log)})))
          (Thread/sleep 100)
          (recur (dec remaining)))))))

(defn lose-unsynced-writes!
  "Clear the lazyfs cache and wait for its answer, so the server never
  restarts on a cache still being cleared. No-op without lazyfs."
  [state]
  (when-let [{:keys [^RandomAccessFile answers paths]} (:lazyfs @state)]
    (spit (:fifo paths) "lazyfs::clear-cache\n")
    (let [answer (future (.readLine answers))]
      (when-not (= "finished::clear-cache" (deref answer 30000 nil))
        (throw (ex-info "lazyfs did not answer the cache clear"
                        {:log (str (:daemon-log paths))}))))))

(defn unmount-lazyfs!
  "Unmount and stop lazyfs. An unmount may fail, before the mount is
  up or after the daemon died; the daemon is stopped either way, and
  a mount a dead daemon left behind is detached lazily."
  [state]
  (when-let [{:keys [^Process process ^RandomAccessFile answers paths]}
             (:lazyfs @state)]
    (let [mount (str (:mount paths))
          unmount (fn [& flags]
                    (try (apply exec! "fusermount3" (concat flags [mount]))
                         true
                         (catch clojure.lang.ExceptionInfo _ false)))]
      (unmount "-u")
      (when-not (.waitFor process 30 TimeUnit/SECONDS)
        (.destroyForcibly process)
        (.waitFor process)
        (unmount "-uz")))
    (.close answers)
    (swap! state dissoc :lazyfs)))

(defn ready? [test log-file log-offset]
  (and (str/includes? (subs (slurp log-file) log-offset) "become-leader")
       (try
         (with-open [conn (client/open test "127.0.0.1")]
           (boolean (j/execute-one! conn ["SELECT 1"])))
         (catch java.sql.SQLException _ false))))

(defn start! [state test]
  (when-not (some-> (:process @state) .isAlive)
    (let [dir (:directory @state)
          data (or (some-> (:lazyfs @state) :paths :mount) dir)
          log-file (io/file dir "server.log")
          log-offset (if (.exists log-file) (count (slurp log-file)) 0)
          command [(:binary test)
                   "--raft=id=1"
                   (str "--listen=sql_port=" (:port test)
                        ";raft_port=" (inc (:port test))
                        ";client_port=" (+ 2 (:port test)))
                   (str "--store=dir_path=" (io/file data "sst"))
                   (str "--wal=dir=" (io/file data "wal"))
                   "--memory=total-bytes=536870912"]
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
          hook (Thread. ^Runnable #(do (stop! state true)
                                       (unmount-lazyfs! state)))]
      (.mkdirs dir)
      (reset! state {:directory dir :hook hook})
      (.addShutdownHook (Runtime/getRuntime) hook)
      (when (:lazyfs? test) (mount-lazyfs! state test))
      (start! state test)))
  (teardown! [this test node]
    (stop! state false)
    (unmount-lazyfs! state)
    (when-let [hook (:hook @state)]
      (.removeShutdownHook (Runtime/getRuntime) hook)))
  db/Process
  (start! [this test node] (start! state test))
  (kill! [this test node]
    (stop! state true)
    (lose-unsynced-writes! state))
  db/Pause
  (pause! [this test node] (signal! state "STOP"))
  (resume! [this test node] (signal! state "CONT")))
