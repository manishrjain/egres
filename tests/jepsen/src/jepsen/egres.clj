;; Copyright 2020--2026 Jepsen, LLC
;; SPDX-License-Identifier: EPL-2.0
;; Adapted from the Jepsen PostgreSQL suite and Jepsen core; sources and
;; modifications: tests/jepsen/LICENSE-NOTES.md.

(ns jepsen.egres
  (:gen-class)
  (:require [clojure.java.io :as io]
            [jepsen.checker :as checker]
            [jepsen.cli :as cli]
            [jepsen.db :as db]
            [jepsen.egres.client :as client]
            [jepsen.egres.db :as local]
            [jepsen.egres.internal :as internal]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.tests :as tests]
            [jepsen.tests.cycle.append :as append]
            [jepsen.tests.cycle.wr :as wr]))

(defrecord FinalReads []
  gen/Generator
  (op [this test ctx]
    ;; Include the highest key and the final partial batch. Final reads
    ;; expose committed writes that no concurrent transaction observed.
    (gen/op (for [ks (partition-all 8 (range (inc (:max-key ctx 0))))]
              {:type :invoke :f :txn :final? true
               :value (mapv #(vector :r % nil) ks)}) test ctx))
  (update [this test ctx event] this))

;; The micro-ops that write, across the workloads.
(def write-ops #{:append :w :insert :update})

(defn progress-checker []
  (reify checker/Checker
    (check [_ test history opts]
      (let [ok (filter #(= :ok (:type %)) history)
            writes (count (filter #(some (fn [mop] (write-ops (first mop)))
                                         (:value %)) ok))
            finals (filter #(and (:final? %) (not= :invoke (:type %)))
                           history)
            failed-finals (remove #(= :ok (:type %)) finals)
            exceptions (filter :exception history)
            fault (:fault-mode test :none)
            faults (count (filter #(and (= :nemesis (:process %))
                                        (= fault (:f %))
                                        (= fault (:value %))) history))]
        {:valid? (boolean
                   (and (pos? writes)
                        (or (not (:final-reads? test true)) (seq finals))
                        (empty? failed-finals)
                        (empty? exceptions)
                        (or (= :none fault) (pos? faults))))
         :committed-write-transactions writes
         :final-read-transactions (count finals)
         :failed-final-reads (vec failed-finals)
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

(defn workload
  "The generator, checker and generator wrapper for --workload. append
  and rw are Jepsen's Elle workloads, checked for snapshot isolation;
  internal is the own-writes check, with no final reads (its keys are
  not Elle's)."
  [opts]
  (let [elle {:key-count (:key-count opts)
              :min-txn-length 2
              :max-txn-length (:max-txn-length opts)
              :max-writes-per-key (:max-writes-per-key opts)
              :cycle-search-timeout 10000
              :consistency-models [:snapshot-isolation]}]
    (case (:workload opts)
      :append (assoc (append/test elle) :final-reads? true)
      :rw (assoc (wr/test (assoc elle :min-txn-length 1)) :final-reads? true)
      :internal {:generator (internal/generator)
                 :checker (internal/checker)
                 :wrap-generator identity
                 :final-reads? false})))

(defn egres-test [opts]
  (when-not (= ["127.0.0.1"] (:nodes opts))
    (throw (ex-info "This harness runs one local Egres node" {})))
  (when-not (pos? (:concurrency opts))
    (throw (ex-info "Concurrency must be positive" {})))
  (let [binary (.getCanonicalPath (io/file (:binary opts)))
        _ (when-not (.canExecute (io/file binary))
            (throw (ex-info "Build Egres in ReleaseSafe first"
                            {:binary binary})))
        workload (workload opts)
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
                             {:type :info :f heal}]))
        checker (:checker workload)]
    (merge tests/noop-test opts
           {:name (str "egres-" (name (:workload opts)) "-"
                       (name (:access opts)) "-" (:query-mode opts) "-"
                       (name fault))
            :binary binary
            :fault-mode fault
            :final-reads? (:final-reads? workload)
            :ssh {:dummy? true}
            :db database
            :client (client/->Client nil)
            :nemesis (if hurt (process-nemesis database) nemesis/noop)
            :checker (checker/compose
                       (cond-> {:stats (checker/stats)
                                :exceptions (checker/unhandled-exceptions)
                                :progress (progress-checker)}
                         (= :internal (:workload opts))
                         (assoc :internal checker)
                         (not= :internal (:workload opts))
                         (assoc :snapshot-isolation checker)))
            :generator
            ((:wrap-generator workload)
             (gen/phases
               (->> (:generator workload)
                    (gen/stagger (/ (:rate opts)))
                    (gen/nemesis faults)
                    (gen/time-limit (:time-limit opts)))
               (when heal
                 (gen/nemesis (gen/once {:type :info :f heal})))
               (when (:final-reads? workload)
                 (gen/clients (->FinalReads)))))})))

(def cli-opts
  [[nil "--binary PATH" "ReleaseSafe Egres binary"
    :default "../../zig-out/bin/egres"]
   [nil "--port PORT" "SQL port; the next two ports are Raft and admin"
    :default 13210 :parse-fn parse-long
    :validate [#(<= 1024 % 65533) "Must be between 1024 and 65533"]]
   [nil "--workload NAME" (str "append (Elle list-append), rw (Elle "
                              "registers) or internal (own writes)")
    :default :append :parse-fn keyword
    :validate [#{:append :rw :internal} "append, rw, or internal"]]
   [nil "--query-mode MODE" "PostgreSQL JDBC query mode"
    :default "simple" :validate [#{"simple" "extended"} "simple or extended"]]
   [nil "--access PATH" "Read/write lookup path"
    :default :primary :parse-fn keyword
    :validate [#{:primary :secondary :scan} "primary, secondary, or scan"]]
   [nil "--table-count N" "Tables to distribute keys across"
    :default 4 :parse-fn parse-long :validate [pos? "Must be positive"]]
   [nil "--key-count N" "Active keys; fewer keys increase contention"
    :default 8 :parse-fn parse-long :validate [pos? "Must be positive"]]
   [nil "--max-txn-length N" "Maximum micro-operations per transaction"
    :default 8 :parse-fn parse-long :validate [#(<= 2 %) "Must be at least 2"]]
   [nil "--max-writes-per-key N" "Writes before rotating a key"
    :default 32 :parse-fn parse-long :validate [pos? "Must be positive"]]
   [nil "--rate HZ" "Target transaction starts per second"
    :default 100 :parse-fn parse-long :validate [pos? "Must be positive"]]
   [nil "--nemesis MODE" (str "Fault injection: none, process kill/restart,"
                             " or pause (SIGSTOP/SIGCONT)")
    :default :none :parse-fn keyword
    :validate [#{:none :kill :pause} "none, kill, or pause"]]
   [nil "--nemesis-interval SECONDS" "Healthy time between faults"
    :default 5 :parse-fn parse-long :validate [pos? "Must be positive"]]
   [nil "--fault-duration SECONDS" "Time a process stays dead or paused"
    :default 1 :parse-fn parse-long :validate [pos? "Must be positive"]]])

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn egres-test
                                       :opt-spec cli-opts})
                   (cli/serve-cmd)) args))
