;; SPDX-License-Identifier: EPL-2.0

(ns jepsen.egres-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.tools.cli :as tools.cli]
            [elle.list-append :as elle]
            [jepsen.checker :as checker]
            [jepsen.egres :as egres]
            [jepsen.egres.client :as client]
            [jepsen.generator :as gen]
            [jepsen.generator.context :as context]
            [jepsen.history :as history])
  (:import (clojure.lang ExceptionInfo)
           (java.sql SQLException)))

(deftest errors-preserve-commit-uncertainty
  (doseq [state ["40001" "40P01" "23505" "72000" "25P02"]]
    (is (true? (:definite? (client/error-fn (SQLException. "abort" state))))))
  (doseq [state ["08001" "08006" "57P01"]]
    (let [error (client/error-fn (SQLException. "lost reply" state))]
      (is (= :io-error (:type error)))
      (is (not (:definite? error)))))
  (doseq [state ["0A000" "42601" "XX000" "42P01" nil]]
    (is (nil? (client/error-fn (SQLException. "unexpected" state)))))
  (is (true? (:definite? (client/error-fn
                           (SQLException. "ERROR: not the leader" "XX000"))))))

(deftest rollback-errors-cannot-reclassify-a-lost-commit
  ;; next.jdbc's shape when ROLLBACK fails after the error it handles.
  (let [wrap (fn [handling]
               (ex-info "Rollback failed"
                        {:rollback (SQLException. "cleanup" "08006")
                         :handling handling}))]
    (is (= :io-error
           (:type (client/error-fn (wrap (SQLException. "lost" "08006"))))))
    (is (true? (:definite? (client/error-fn
                             (wrap (SQLException. "abort" "40001"))))))
    (is (nil? (client/error-fn (wrap (SQLException. "odd" "XX000")))))))

(deftest unknown-ex-info-passes-through-for-the-wrapper
  ;; jepsen.sql's client looks for :type in the data of an ex-info.
  (is (= {:type :jepsen.sql/too-many :critical? true}
         (client/error-fn (ex-info "too many"
                                   {:type :jepsen.sql/too-many
                                    :critical? true})))))

(defn drain-finals
  "Run the final-read generator to completion with `complete`, a
  function from the invoke op to its completion type, returning the
  invocations in order."
  [max-key complete]
  (let [test {:concurrency 2}
        ctx (assoc (context/context test) :max-key max-key)]
    (loop [generator (egres/->FinalReads nil #{} {}) invoked []]
      (if (< 100 (count invoked))
        (throw (ex-info "Final reads never finished" {:invoked invoked}))
        (let [[op generator] (gen/op generator test ctx)]
          (cond
            (nil? op) invoked
            (= :pending op) (recur generator invoked)
            :else (recur (gen/update generator test ctx
                                     (assoc op :type (complete op)))
                         (conj invoked op))))))))

(deftest final-reads-cover-every-key-once-when-they-succeed
  (let [invoked (drain-finals 17 (constantly :ok))
        keys (mapcat (fn [op] (map second (:value op))) invoked)]
    (is (= (range 18) keys))
    (is (= [8 8 2] (map (comp count :value) invoked)))
    (is (every? :final? invoked))
    (is (every? (fn [op] (every? #(= [:r nil] [(first %) (last %)])
                                 (:value op)))
                invoked))))

(deftest final-reads-retry-a-batch-that-did-not-complete
  ;; The first attempt on a worker idle across a restart is :info; the
  ;; retry runs on a fresh connection.
  (let [seen (atom {})
        invoked (drain-finals 9
                              (fn [op]
                                (let [batch (mapv second (:value op))
                                      n (get (swap! seen update batch
                                                    (fnil inc 0))
                                             batch)]
                                  (if (and (= 0 (first batch)) (= 1 n))
                                    :info
                                    :ok))))]
    (is (= [[0 1 2 3 4 5 6 7] [8 9] [0 1 2 3 4 5 6 7]]
           (mapv #(mapv second (:value %)) invoked)))))

(deftest final-reads-give-up-after-the-attempt-limit
  (let [invoked (drain-finals 3 (constantly :fail))]
    (is (= egres/final-read-attempts (count invoked)))))

(defn pair [process type value]
  [{:type :invoke :process process :f :txn
    :value (mapv (fn [[f k v]] [f k (when (= :append f) v)]) value)}
   {:type type :process process :f :txn :value value}])

(defn check-history [events]
  (elle/check {:consistency-models [:snapshot-isolation]}
              (history/history events)))

(deftest checker-rejects-aborted-writes
  (let [result (check-history
                 (concat (pair 0 :fail [[:append 0 1]])
                         (pair 1 :ok [[:r 0 [1]]])))]
    (is (false? (:valid? result)))
    (is (some #{:G1a} (:anomaly-types result)))))

(deftest checker-rejects-nonrepeatable-reads
  (let [result (check-history
                 (concat (pair 0 :ok [[:append 0 1]])
                         (pair 1 :ok [[:r 0 [1]] [:r 0 nil]])))]
    (is (false? (:valid? result)))
    (is (some #{:internal} (:anomaly-types result)))))

(deftest snapshot-isolation-allows-write-skew
  (let [[invoke-a finish-a] (pair 0 :ok [[:r 0 nil] [:append 1 1]])
        [invoke-b finish-b] (pair 1 :ok [[:r 1 nil] [:append 0 1]])
        events (concat [invoke-a invoke-b finish-a finish-b]
                       (pair 2 :ok [[:r 0 [1]] [:r 1 [1]]]))]
    (is (true? (:valid? (check-history events))))))

(def write {:type :ok :value [[:append 0 1]]})

(defn final [type keys]
  [{:type :invoke :final? true :value (mapv #(vector :r % nil) keys)}
   {:type type :final? true :value (mapv #(vector :r % [1]) keys)}])

(deftest empty-or-unobserved-runs-cannot-pass
  (let [check #(checker/check (egres/progress-checker) {} % {})]
    (is (false? (:valid? (check []))))
    (is (false? (:valid? (check [write]))))
    (is (false? (:valid? (check (into [write] (final :info [0 1]))))))
    (is (false? (:valid? (check (into [write {:exception "unexpected"}]
                                      (final :ok [0 1]))))))
    (is (true? (:valid? (check (into [write] (final :ok [0 1]))))))
    ;; A batch that failed once and was read on the retry is covered.
    (is (true? (:valid? (check (-> [write]
                                   (into (final :info [0 1]))
                                   (into (final :ok [0 1])))))))
    (is (= [[2 3]]
           (:unread-final-batches
             (check (-> [write]
                        (into (final :ok [0 1]))
                        (into (final :fail [2 3])))))))))

(deftest crash-test-must-actually-crash
  (let [events (into [write] (final :ok [0]))]
    (is (false? (:valid? (checker/check (egres/progress-checker)
                                        {:fault-mode :kill} events {}))))
    (is (true? (:valid?
                 (checker/check
                   (egres/progress-checker) {:fault-mode :kill}
                   (conj events {:process :nemesis :type :info
                                 :f :kill :value :kill}) {}))))))

(deftest fault-tests-must-inject-their-fault
  (let [events (into [{:type :ok :value [[:w 0 1]]}] (final :ok [0]))
        pause {:process :nemesis :type :info :f :pause :value :pause}]
    (is (false? (:valid? (checker/check (egres/progress-checker)
                                        {:fault-mode :pause} events {}))))
    (is (true? (:valid? (checker/check (egres/progress-checker)
                                       {:fault-mode :pause}
                                       (conj events pause) {}))))
    ;; internal has no final reads to require.
    (is (true? (:valid? (checker/check
                          (egres/progress-checker)
                          {:fault-mode :none :final-reads? false}
                          [{:type :ok :value [[:insert 0 1]]}] {}))))))

(deftest checker-rejects-a-write-lost-on-restart
  ;; Seen on the first crash run, before SQL waited for the boot WAL
  ;; tail: one acknowledged transaction wrote two keys and a final
  ;; read after the restart saw only one of them.
  (let [result (check-history
                 (concat (pair 0 :ok [[:append 0 1] [:append 1 1]])
                         (pair 1 :ok [[:r 0 [1]] [:r 1 nil]])))]
    (is (false? (:valid? result)))
    (is (some #{:G-single-item} (:anomaly-types result)))))

(deftest options-merge-with-upstream-and-default-to-egres
  ;; tools.cli refuses two specs with one id; merging by option string
  ;; keeps only a spec whose string matches upstream's exactly.
  (let [{:keys [options errors]} (tools.cli/parse-opts [] egres/cli-opts)]
    (is (empty? errors))
    (is (= :repeatable-read (:isolation options)))
    (is (= :snapshot-isolation (:expected-consistency-model options)))
    (is (true? (:savepoints options)))
    (is (true? (:secondary-index? options)))
    (is (= [:primary :secondary] (:key-types options)))
    (is (= #{:update-insert-update :on-conflict :copy-on-write}
           (set (:upsert-types options))))
    (is (some #{:decimal} (:encodings options)))
    (is (= 100 (:mop-delay options)))))

(deftest unsupported-workloads-and-indirection-are-refused
  (is (= #{:append :rw :internal} (set (keys egres/workloads))))
  (is (thrown-with-msg? ExceptionInfo #"JOIN"
                        (egres/egres-test {:nodes ["127.0.0.1"]
                                           :concurrency 1
                                           :indirection? true}))))
