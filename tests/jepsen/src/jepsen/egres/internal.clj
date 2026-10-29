;; Copyright 2026 Jepsen, LLC
;; SPDX-License-Identifier: EPL-2.0
;; Adapted from the Jepsen SQL suite; sources and modifications:
;; tests/jepsen/LICENSE-NOTES.md.

(ns jepsen.egres.internal
  "Internal consistency, after upstream jepsen.sql.workload.internal: a
  transaction inserts, updates and reads recent keys, then reads every
  key it touched again, and must observe its own latest write to each."
  (:require [jepsen.checker :as checker]
            [jepsen.generator :as gen]))

(defn- recent-offset
  "How far back from the newest key to reach: mostly 0, rarely a few."
  [newest]
  (if (or (zero? newest) (< (rand) 0.7))
    0
    (rand-int (inc (min newest 5)))))

(defrecord Generator [next-id]
  gen/Generator
  (op [this test ctx]
    ;; Inserts take the newest key, once, so a duplicate never aborts a
    ;; transaction (no savepoints to recover with); updates and reads
    ;; reach back a little.
    (let [length (inc (rand-int (max 1 (quot (:max-txn-length test 8) 2))))]
      (loop [i 0, next-id next-id, txn []]
        (if (< i length)
          (let [k (- next-id (recent-offset next-id))
                roll (rand-int 7)]
            (cond
              (zero? roll) (recur (inc i) (inc next-id)
                                  (conj txn [:insert next-id (rand-int 100)]))
              (< roll 5) (recur (inc i) next-id
                                (conj txn [:update k (rand-int 100)]))
              :else (recur (inc i) next-id (conj txn [:r k nil]))))
          (let [reads (mapv (fn [[_ k _]] [:r k nil]) (shuffle txn))]
            [(gen/fill-in-op {:f :txn :value (into txn reads)} ctx)
             (Generator. next-id)])))))
  (update [this test ctx event] this))

(defn generator []
  (Generator. 0))

(defn anomaly
  "The first micro-op of an ok transaction that failed to observe the
  transaction's own most recent write or read of its key, or nil."
  [op]
  (when (= :ok (:type op))
    (loop [state {}, mops (:value op)]
      (when-let [[f k v :as mop] (first mops)]
        (if (and (= :r f) (contains? state k) (not= v (state k)))
          {:op op :mop mop :key k :expected (state k) :actual v}
          (recur (assoc state k v) (rest mops)))))))

(defn checker []
  (reify checker/Checker
    (check [_ test history opts]
      (let [txns (filter #(and (= :ok (:type %)) (= :txn (:f %))) history)
            errors (keep anomaly txns)]
        {:valid? (empty? errors)
         :errors (vec errors)
         :error-count (count errors)
         :txn-count (count txns)}))))
