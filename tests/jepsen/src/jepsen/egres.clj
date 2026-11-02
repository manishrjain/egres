;; Copyright 2020--2026 Jepsen, LLC
;; SPDX-License-Identifier: EPL-2.0
;; Adapted from the Jepsen PostgreSQL suite and Jepsen core; sources and
;; modifications: tests/jepsen/LICENSE-NOTES.md.

(ns jepsen.egres
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.cli :as cli]
            [jepsen.client :as jc]
            [jepsen.db :as db]
            [jepsen.egres.client :as client]
            [jepsen.egres.db :as local]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.sql :as sql]
            [jepsen.sql.encoding :as encoding]
            [jepsen.tests :as tests]
            [next.jdbc :as j]))

(def workloads
  "Upstream's SQL workloads over Egres connections. internal-sim is
  left out: its generated schemas declare `TEXT COLLATE unicode`
  columns and compare strings under that collation."
  (dissoc (sql/workloads {:open client/open :error-fn client/error-fn})
          :internal-sim))

(defrecord IndexedClient [client node]
  jc/Client
  (open! [this test node]
    (assoc this :client (jc/open! client test node) :node node))
  (setup! [this test]
    (jc/setup! client test)
    ;; Upstream's sk is a bare column that a secondary lookup scans for.
    ;; --secondary-index puts Egres's bitmap index under it, on each
    ;; table the workload made: rw makes one per pair of encodings, up
    ;; to --table-count. internal has no sk.
    (when (and (:secondary-index? test) (not= :internal (:workload test)))
      (with-open [conn (client/open test node)]
        (doseq [{:keys [table]} (:tables client)]
          (j/execute! conn [(str "CREATE INDEX IF NOT EXISTS " table
                                 "_sk ON " table " (sk)")])))))
  (invoke! [this test op] (jc/invoke! client test op))
  (teardown! [this test] (jc/teardown! client test))
  (close! [this test] (jc/close! client test)))

(def final-read-attempts
  "Reads of one batch of keys before the batch is given up and the run
  fails. A worker idle across the last restart finds its socket dead on
  its first statement, which completes the batch as :info; the next
  attempt runs on a fresh connection."
  4)

(defn final-batches
  "The keys written so far, eight per transaction, the highest and the
  final partial batch included."
  [ctx]
  (mapv vec (partition-all 8 (range (inc (:max-key ctx 0))))))

(defrecord FinalReads [pending issued tries]
  ;; pending: batches not yet sent, nil before the first op; issued: the
  ;; batches in flight; tries: batch -> attempts so far.
  gen/Generator
  (op [this test ctx]
    (let [pending (or pending (final-batches ctx))]
      (cond
        (seq pending)
        (let [batch (first pending)
              op (gen/fill-in-op {:f :txn :final? true
                                  :value (mapv #(vector :r % nil) batch)}
                                 ctx)]
          (if (= :pending op)
            [:pending (assoc this :pending pending)]
            [op (assoc this
                       :pending (vec (rest pending))
                       :issued (conj issued batch)
                       :tries (update tries batch (fnil inc 0)))]))

        (seq issued) [:pending (assoc this :pending pending)]
        :else nil)))
  (update [this test ctx event]
    (if (and (:final? event) (not= :invoke (:type event)))
      (let [batch (mapv second (:value event))
            this (assoc this :issued (disj issued batch))]
        (if (or (= :ok (:type event))
                (<= final-read-attempts (get tries batch 0)))
          this
          (assoc this :pending (conj (or pending []) batch))))
      this)))

;; The micro-ops that write, across the workloads.
(def write-ops #{:append :w :insert :update})

(defn progress-checker []
  (reify checker/Checker
    (check [_ test history opts]
      (let [ok (filter #(= :ok (:type %)) history)
            writes (count (filter #(some (fn [mop] (write-ops (first mop)))
                                         (:value %)) ok))
            finals (filter :final? history)
            batch (fn [op] (mapv second (:value op)))
            wanted (set (map batch finals))
            read (set (map batch (filter #(= :ok (:type %)) finals)))
            unread (vec (sort (remove read wanted)))
            exceptions (filter :exception history)
            fault (:fault-mode test :none)
            faults (count (filter #(and (= :nemesis (:process %))
                                        (= fault (:f %))
                                        (= fault (:value %))) history))]
        {:valid? (boolean
                   (and (pos? writes)
                        (or (not (:final-reads? test true)) (seq wanted))
                        (empty? unread)
                        (empty? exceptions)
                        (or (= :none fault) (pos? faults))))
         :committed-write-transactions writes
         :final-read-batches (count wanted)
         :unread-final-batches unread
         :unhandled-exceptions (count exceptions)
         :process-faults faults}))))

(defn process-nemesis
  "kill/start and pause/resume of the one Egres process."
  [database]
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (case (:f op)
        :kill (db/kill! database test "127.0.0.1")
        :start (db/start! database test "127.0.0.1")
        :pause (db/pause! database test "127.0.0.1")
        :resume (db/resume! database test "127.0.0.1"))
      (assoc op :type :info :value (:f op)))
    (teardown! [this test])))

(defn egres-test [opts]
  (when-not (= ["127.0.0.1"] (:nodes opts))
    (throw (ex-info "This harness runs one local Egres node" {})))
  (when-not (pos? (:concurrency opts))
    (throw (ex-info "Concurrency must be positive" {})))
  (when (:indirection? opts)
    (throw (ex-info "Indirection reads through a JOIN, which Egres does not run"
                    {})))
  (let [binary (.getCanonicalPath (io/file (:binary opts)))
        _ (when-not (.canExecute (io/file binary))
            (throw (ex-info "Build Egres in ReleaseSafe first"
                            {:binary binary})))
        workload ((workloads (:workload opts)) opts)
        ;; Elle's workloads track the highest key for the final reads;
        ;; internal has its own keys and no final reads.
        final-reads? (not= :internal (:workload opts))
        database (local/->LocalDB (atom nil))
        fault (:nemesis opts)
        ;; The fault and its undo: a kill is followed by a start, a
        ;; pause by a resume, after --fault-duration.
        [hurt heal] (case fault
                      :kill [:kill :start]
                      :pause [:pause :resume]
                      :none nil)
        faults (when hurt
                 (gen/cycle [(gen/sleep (:nemesis-interval opts))
                             {:type :info :f hurt}
                             (gen/sleep (:fault-duration opts))
                             {:type :info :f heal}]))]
    (merge tests/noop-test opts
           {:name (str "egres-" (name (:workload opts)) "-"
                       (str/join "+" (map name (:key-types opts))) "-"
                       (:query-mode opts) "-" (name fault))
            :binary binary
            :fault-mode fault
            :final-reads? final-reads?
            :ssh {:dummy? true}
            :db database
            :client (->IndexedClient (:client workload) nil)
            :nemesis (if hurt (process-nemesis database) nemesis/noop)
            :checker (checker/compose
                       {:stats (checker/stats)
                        :exceptions (checker/unhandled-exceptions)
                        :progress (progress-checker)
                        :workload (:checker workload)})
            :generator
            ((:wrap-generator workload identity)
             (gen/phases
               (->> (:generator workload)
                    (gen/stagger (/ (:rate opts)))
                    (gen/nemesis faults)
                    (gen/time-limit (:time-limit opts)))
               (when heal
                 (gen/nemesis (gen/once {:type :info :f heal})))
               (when final-reads?
                 (gen/clients (->FinalReads nil #{} {})))))})))

(def encodings
  "Upstream's value encodings, all six: decimal runs on `numeric(32,
  16)`."
  (mapv :name encoding/all-encodings))

(def egres-opts
  "Options added to, or given Egres's defaults over, jepsen.sql's."
  [[nil "--binary PATH" "ReleaseSafe Egres binary"
    :default "../../zig-out/bin/egres"]
   [nil "--port PORT" "SQL port; the next two ports are Raft and admin"
    :default 13210 :parse-fn parse-long
    :validate [#(<= 1024 % 65533) "Must be between 1024 and 65533"]]
   [nil "--workload NAME" (str "append (Elle list-append), rw (Elle "
                              "registers) or internal (own writes)")
    :default :append :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]
   [nil "--query-mode MODE" "PostgreSQL JDBC query mode"
    :default "simple" :validate [#{"simple" "extended"} "simple or extended"]]
   [nil "--[no-]secondary-index"
    "Index the secondary key sk, which upstream leaves a plain column"
    :id :secondary-index? :default true]
   [nil "--table-count N" "Tables to distribute keys across"
    :default 4 :parse-fn parse-long :validate [pos? "Must be positive"]]
   ["-i" "--isolation LEVEL"
    "Isolation level asked of each transaction; Egres runs snapshot isolation"
    :default :repeatable-read :parse-fn keyword
    :validate [#{:read-uncommitted :read-committed :repeatable-read
                 :serializable}
               (str "read-uncommitted, read-committed, repeatable-read or "
                    "serializable")]]
   [nil "--expected-consistency-model MODEL" "The model Elle checks for"
    :default :snapshot-isolation :parse-fn keyword]
   [nil "--encodings ENCODING_NAMES" "Comma-separated value encodings for rw"
    :default encodings :parse-fn sql/parse-comma-kws
    :validate [(partial every? (set encodings)) (cli/one-of encodings)]]
   ["-r" "--rate HZ" "Target transaction starts per second"
    :default 100 :parse-fn parse-long :validate [pos? "Must be positive"]]
   [nil "--nemesis MODE" (str "Fault injection: none, process kill/restart,"
                             " or pause (SIGSTOP/SIGCONT)")
    :default :none :parse-fn keyword
    :validate [#{:none :kill :pause} "none, kill, or pause"]]
   [nil "--fault-duration SECONDS" "Time a process stays dead or paused"
    :default 1 :parse-fn parse-long :validate [pos? "Must be positive"]]])

(def cli-opts
  (cli/merge-opt-specs sql/cli-opts egres-opts))

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn egres-test
                                       :opt-spec cli-opts})
                   (cli/serve-cmd)) args))
