;; SPDX-License-Identifier: EPL-2.0

(ns jepsen.egres-test
  (:require [clojure.test :refer [deftest is]]
            [elle.list-append :as elle]
            [jepsen.checker :as checker]
            [jepsen.egres :as egres]
            [jepsen.egres.client :as client]
            [jepsen.egres.internal :as internal]
            [jepsen.generator :as gen]
            [jepsen.generator.context :as context]
            [jepsen.history :as history])
  (:import (java.sql SQLException)))

(deftest errors-preserve-commit-uncertainty
  (doseq [state ["40001" "40P01" "23505" "72000"]]
    (is (= :fail (client/error-type (SQLException. "abort" state)))))
  (doseq [state ["08001" "08006" "57P01"]]
    (is (= :info (client/error-type (SQLException. "lost reply" state)))))
  (doseq [state ["0A000" "42601" "XX000" "25P02" nil]]
    (is (nil? (client/error-type (SQLException. "unexpected" state)))))
  (is (= :fail (client/error-type
                 (SQLException. "ERROR: not the leader" "XX000")))))

(deftest transactions-do-not-acknowledge-a-lost-commit
  (let [commands (atom [])]
    (with-redefs [client/execute!
                  (fn [_ sql & _]
                    (swap! commands conj sql)
                    (when (= sql "COMMIT")
                      (throw (SQLException. "lost reply" "08006"))))
                  client/read! (fn [& _] [1])]
      (is (thrown? SQLException
                   (client/transact! nil {} {:value [[:r 0 nil]]})))
      (is (= ["BEGIN ISOLATION LEVEL REPEATABLE READ" "COMMIT" "ROLLBACK"]
             @commands)))))

(deftest duplicate-insert-aborts-the-whole-transaction
  (let [commands (atom [])]
    (with-redefs [client/execute!
                  (fn [_ sql & _]
                    (swap! commands conj sql)
                    (if (.startsWith sql "INSERT")
                      (throw (SQLException. "duplicate" "23505"))
                      0))]
      (is (thrown? SQLException
                   (client/transact!
                     nil {:table-count 1 :access :primary}
                     {:value [[:append 0 1] [:r 0 nil]]})))
      (is (= "ROLLBACK" (last @commands)))
      (is (not-any? #{"COMMIT"} @commands)))))

(deftest registers-update-then-insert-with-bound-parameters
  (let [commands (atom [])]
    (with-redefs [client/execute!
                  (fn [_ sql & params]
                    (swap! commands conj [sql (vec params)])
                    (if (.startsWith sql "UPDATE") 0 1))]
      (is (= [:w 7 3] (client/mop! nil {:table-count 1 :access :primary
                                        :workload :rw}
                                   [:w 7 3])))
      (is (= [["UPDATE jepsen_rw_0 SET val = ? WHERE id = ?" [3 7]]
              ["INSERT INTO jepsen_rw_0 (id, sk, val) VALUES (?, ?, ?)"
               [7 7 3]]]
             @commands)))))

(deftest internal-updates-of-absent-keys-take-no-effect
  (with-redefs [client/execute! (fn [_ sql & _] 0)]
    (is (nil? (client/mop! nil {:table-count 1 :workload :internal}
                           [:update 7 3])))))

(deftest rollback-errors-cannot-reclassify-a-lost-commit
  (with-redefs [client/execute!
                (fn [_ sql & _]
                  (when (= sql "COMMIT")
                    (throw (SQLException. "lost reply" "08006"))))
                client/rollback!
                (fn [_] (throw (SQLException. "cleanup failed" "40001")))
                client/read! (fn [& _] [1])]
    (try
      (client/transact! nil {} {:value [[:r 0 nil]]})
      (is false "A lost COMMIT must throw")
      (catch SQLException error
        (is (= :info (client/error-type error)))
        (is (= 1 (count (.getSuppressed error))))))))

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

(deftest empty-or-unobserved-runs-cannot-pass
  (let [check #(checker/check (egres/progress-checker) {} % {})
        write {:type :ok :value [[:append 0 1]]}
        final {:type :ok :final? true :value [[:r 0 [1]]]}]
    (is (false? (:valid? (check []))))
    (is (not (:valid? (check [write]))))
    (is (false? (:valid? (check [write (assoc final :type :info)]))))
    (is (false? (:valid? (check [write final {:exception "unexpected"}]))))
    (is (true? (:valid? (check [write final]))))))

(deftest crash-test-must-actually-crash
  (let [events [{:type :ok :value [[:append 0 1]]}
                {:type :ok :final? true :value [[:r 0 [1]]]}]]
    (is (false? (:valid? (checker/check (egres/progress-checker)
                                        {:fault-mode :kill} events {}))))
    (is (true? (:valid?
                 (checker/check
                   (egres/progress-checker) {:fault-mode :kill}
                   (conj events {:process :nemesis :type :info
                                 :f :kill :value :kill}) {}))))))

(deftest internal-checker-requires-a-txn-to-see-its-own-writes
  (let [ok (fn [value] {:type :ok :f :txn :value value})
        check #(checker/check (internal/checker) {} % {})]
    (is (true? (:valid? (check [(ok [[:insert 1 5] [:update 1 6] [:r 1 6]])
                                (ok [[:r 2 nil] [:update 2 1] [:r 2 1]])
                                (ok [[:r 3 4] [:r 3 4]])
                                {:type :fail :f :txn
                                 :value [[:update 1 9] [:r 1 6]]}]))))
    (let [result (check [(ok [[:insert 1 5] [:r 1 nil]])])]
      (is (false? (:valid? result)))
      (is (= 5 (:expected (first (:errors result)))))
      (is (nil? (:actual (first (:errors result))))))
    (is (false? (:valid? (check [(ok [[:r 3 4] [:r 3 5]])]))))))

(deftest internal-generator-inserts-each-key-once
  (let [test {:max-txn-length 8 :concurrency 1}
        ctx (context/context test)
        ops (loop [g (internal/generator) n 0 ops []]
              (if (= n 200)
                ops
                (let [[op g] (gen/op g test ctx)]
                  (recur g (inc n) (conj ops op)))))
        inserted (for [op ops [f k] (:value op) :when (= :insert f)] k)]
    (is (= (count inserted) (count (set inserted))))
    (is (every? (fn [op] (<= (count (:value op)) 8)) ops))
    (is (every? (fn [op] (every? #(= :r (first %))
                                 (drop (/ (count (:value op)) 2)
                                       (:value op))))
                ops))))

(deftest fault-tests-must-inject-their-fault
  (let [events [{:type :ok :value [[:w 0 1]]}
                {:type :ok :final? true :value [[:r 0 1]]}]
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
