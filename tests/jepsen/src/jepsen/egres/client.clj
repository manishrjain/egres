;; Copyright 2020--2026 Jepsen, LLC
;; SPDX-License-Identifier: EPL-2.0
;; Adapted from the Jepsen PostgreSQL suite's client; sources and
;; modifications: tests/jepsen/LICENSE-NOTES.md.

(ns jepsen.egres.client
  "Connections to the local Egres and its errors, in the shape
  jepsen.sql's workloads take them: what jepsen.postgres.client is to
  PostgreSQL."
  (:require [next.jdbc :as j])
  (:import (clojure.lang ExceptionInfo)
           (java.sql SQLException)))

(defn open
  "A pgjdbc connection to Egres on (:port test), in the test's
  --query-mode. Every statement is a prepared statement with `?`
  parameters."
  [test node]
  ;; binaryTransfer makes pgjdbc send integer and float binds in binary,
  ;; as applications do; Egres decodes them by the declared type.
  ;; Results stay text: pgjdbc asks for binary results only for
  ;; server-named statements, and prepareThreshold 0 names none.
  (j/get-connection {:dbtype "postgresql"
                     :host node
                     :port (:port test)
                     :dbname "egres"
                     :user "jepsen"
                     :sslmode "disable"
                     :connectTimeout 2
                     :socketTimeout 5
                     :assumeMinServerVersion "13"
                     :binaryTransfer true
                     :prepareThreshold 0
                     :preferQueryMode (:query-mode test)}))

(def failed-states
  "SQLSTATEs of a statement Egres rejected, taking its transaction
  down: a definite failure. 25P02 is any statement after one of the
  others, before the block ends."
  {"40001" :could-not-serialize
   "40P01" :deadlock
   "23505" :duplicate-key-value
   "72000" :snapshot-too-old
   "25P02" :aborted})

(defn error-fn
  "The error map jepsen.sql expects for an exception Egres is known to
  raise, nil for any other. A definite error failed the operation; a
  transport error leaves it indeterminate, a lost COMMIT reply above
  all."
  [^Exception e]
  (condp instance? e
    SQLException
    (let [state (.getSQLState ^SQLException e)
          msg (.getMessage e)]
      (cond
        (contains? failed-states state)
        {:type (failed-states state) :msg msg :definite? true}

        ;; Writes before leadership is established are rejected with
        ;; XX000. Only this known pre-commit rejection; another internal
        ;; error must surface as an unhandled exception.
        (and (= "XX000" state) (= "ERROR: not the leader" msg))
        {:type :not-leader :definite? true}

        (or (and state (.startsWith ^String state "08"))
            (#{"57P01" "57P02" "57P03"} state))
        {:type :io-error :msg msg}

        :else nil))

    ExceptionInfo
    (let [data (ex-data e)]
      ;; next.jdbc wraps a failed ROLLBACK around the error it was
      ;; cleaning up after; that error is the one to classify, so a
      ;; dead socket at ROLLBACK cannot turn a lost COMMIT reply into
      ;; a definite failure.
      (if (and (:rollback data) (:handling data))
        (recur (:handling data))
        data))

    nil))
