;; Copyright 2020--2026 Jepsen, LLC
;; SPDX-License-Identifier: EPL-2.0
;; Adapted from the Jepsen PostgreSQL and SQL suites; sources and
;; modifications: tests/jepsen/LICENSE-NOTES.md.

(ns jepsen.egres.client
  (:require [clojure.string :as str]
            [jepsen.client :as client])
  (:import (java.sql Connection DriverManager PreparedStatement
                     SQLException)
           (java.util Properties)))

(defn connect! [test]
  (Class/forName "org.postgresql.Driver")
  (let [props (Properties.)]
    ;; binaryTransfer makes pgjdbc send integer and float binds in
    ;; binary, as applications do; Egres decodes them by the declared
    ;; type. Results stay text: pgjdbc asks for binary results only
    ;; for server-named statements, and prepareThreshold 0 names none.
    (doseq [[k v] {"user" "jepsen"
                   "sslmode" "disable"
                   "connectTimeout" "2"
                   "socketTimeout" "5"
                   "assumeMinServerVersion" "13"
                   "binaryTransfer" "true"
                   "prepareThreshold" "0"
                   "preferQueryMode" (:query-mode test)}]
      (.setProperty props k v))
    (DriverManager/getConnection
      (str "jdbc:postgresql://127.0.0.1:" (:port test) "/egres") props)))

(defn bind! [^PreparedStatement stmt params]
  (doseq [[i param] (map-indexed vector params)]
    (.setObject stmt (inc i) param)))

(defn execute!
  "Run a statement that returns no rows, with `?` bind parameters, and
  return its update count. In extended mode this is pgjdbc's one-row
  Execute, the shape every JDBC application sends."
  [^Connection conn sql & params]
  (with-open [stmt (.prepareStatement conn sql)]
    (bind! stmt params)
    (.executeUpdate stmt)))

(defn query-one!
  "Run a query with `?` bind parameters and return its one row's first
  column as a string, nil for no row. More than one row is an error:
  every lookup here is by a unique key."
  [^Connection conn sql & params]
  (with-open [stmt (.prepareStatement conn sql)]
    (bind! stmt params)
    (with-open [rows (.executeQuery stmt)]
      (when (.next rows)
        (let [value (.getString rows 1)]
          (when (.next rows)
            (throw (ex-info "Multiple rows for one logical key" {:sql sql})))
          value)))))

(defn table-for [test k]
  (str "jepsen_" (name (:workload test :append)) "_"
       (mod k (:table-count test))))

;; The lookup column: the primary key, or the secondary `sk` (indexed or
;; scanned, per --access).
(defn key-column [test]
  (if (= :primary (:access test)) "id" "sk"))

(defn parse-list [value]
  (when (nil? value)
    (throw (ex-info "A stored list must not be NULL" {})))
  (mapv #(Long/parseLong %) (str/split value #",")))

(defn read!
  "The value of key `k`: a list for append, a long for rw and internal,
  nil for an absent key."
  [conn test k]
  (let [value (query-one! conn (str "SELECT val FROM " (table-for test k)
                                    " WHERE " (key-column test) " = ?")
                          (long k))]
    (case (:workload test :append)
      :append (when value (parse-list value))
      (some-> value Long/parseLong))))

(defn append! [conn test k value]
  ;; SQL-side concatenation preserves concurrent appends without adding
  ;; client-side reads or locks that could hide isolation anomalies.
  (let [table (table-for test k)
        updated (execute! conn (str "UPDATE " table " SET val = val || ?"
                                    " WHERE " (key-column test) " = ?")
                          (str "," (long value)) (long k))]
    (case updated
      1 nil
      0 (when-not (= 1 (execute! conn (str "INSERT INTO " table
                                           " (id, sk, val)"
                                           " VALUES (?, ?, ?)")
                                 (long k) (long k) (str (long value))))
          (throw (ex-info "Insert did not affect one row" {:key k})))
      (throw (ex-info "Update affected multiple rows" {:key k}))))
  value)

(defn write!
  "Set register `k` to `value`: UPDATE, then INSERT when the key is new,
  as upstream's update-insert-update strategy does without savepoints.
  A duplicate-key race on the INSERT aborts the transaction (23505)."
  [conn test k value]
  (let [table (table-for test k)
        updated (execute! conn (str "UPDATE " table " SET val = ?"
                                    " WHERE " (key-column test) " = ?")
                          (long value) (long k))]
    (case updated
      1 nil
      0 (when-not (= 1 (execute! conn (str "INSERT INTO " table
                                           " (id, sk, val)"
                                           " VALUES (?, ?, ?)")
                                 (long k) (long k) (long value)))
          (throw (ex-info "Insert did not affect one row" {:key k})))
      (throw (ex-info "Update affected multiple rows" {:key k}))))
  value)

(defn mop!
  "One micro-operation, completed. An internal-workload UPDATE of an
  absent key took no effect and completes as nil, which transact!
  drops, as upstream does."
  [conn test [f k v]]
  (case f
    :r [f k (read! conn test k)]
    :append [f k (append! conn test k v)]
    :w [f k (write! conn test k v)]
    :insert (do (execute! conn (str "INSERT INTO " (table-for test k)
                                    " (id, sk, val) VALUES (?, ?, ?)")
                          (long k) (long k) (long v))
                [f k v])
    :update (when (pos? (execute! conn (str "UPDATE " (table-for test k)
                                            " SET val = ? WHERE id = ?")
                                  (long v) (long k)))
              [f k v])))

(defn error-type [^SQLException error]
  (let [state (.getSQLState error)]
    (cond
      (#{"40001" "40P01" "23505" "72000"} state) :fail
      ;; Egres rejects writes before leadership is established with XX000.
      ;; Match only this known pre-commit rejection, not other server errors.
      (and (= "XX000" state)
           (= "ERROR: not the leader" (.getMessage error))) :fail
      (or (and state (str/starts-with? state "08"))
          (#{"57P01" "57P02" "57P03"} state)) :info
      :else nil)))

(defn rollback! [^Connection conn]
  (try
    (execute! conn "ROLLBACK")
    (catch SQLException _ (.close conn))))

(defn begin! [conn]
  ;; Explicit messages avoid JDBC combining BEGIN with the first query;
  ;; Egres currently executes only one statement per simple-query message.
  ;; A transport error here is a definite failure, not indeterminate:
  ;; nothing that could commit has been sent yet. Typically a socket to
  ;; a server that was killed while this worker sat idle.
  (try
    (execute! conn "BEGIN ISOLATION LEVEL REPEATABLE READ")
    (catch SQLException error
      (throw (ex-info "BEGIN failed" {:definite true} error)))))

(defn transact! [conn test op]
  (try
    (begin! conn)
    (let [value (->> (:value op)
                     (mapv (partial mop! conn test))
                     (filterv some?))]
      (execute! conn "COMMIT")
      (assoc op :type :ok :value value))
    (catch Exception error
      ;; Cleanup must not turn a lost COMMIT reply into a definite abort.
      (try
        (rollback! conn)
        (catch Exception cleanup (.addSuppressed error cleanup)))
      (throw error))))

(defn create-tables! [conn test]
  ;; Upstream's layout: a NOT NULL key, a NOT NULL secondary key, and the
  ;; value, text for append's lists and bigint for registers.
  (let [val-type (if (= :append (:workload test :append)) "text" "bigint")]
    (doseq [idx (range (:table-count test))]
      (let [table (table-for test idx)]
        (execute! conn (str "CREATE TABLE " table
                            " (id bigint NOT NULL PRIMARY KEY,"
                            " sk bigint NOT NULL, val " val-type ")"))
        (when (= :secondary (:access test))
          (execute! conn (str "CREATE INDEX " table "_sk ON " table
                              " (sk)")))))))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (atom (connect! test))))
  (setup! [this test]
    (create-tables! @conn test)
    this)
  (invoke! [this test op]
    (try
      ;; isClosed does not notice a server that died under an idle
      ;; socket. A final read runs right after the last restart, on
      ;; workers that may have been idle across it, so it always gets a
      ;; fresh connection; it is read-only, so nothing is lost by that.
      (when (or (.isClosed ^Connection @conn) (:final? op))
        (.close ^Connection @conn)
        (reset! conn (connect! test)))
      (transact! @conn test op)
      (catch Exception thrown
        (let [definite (and (instance? clojure.lang.ExceptionInfo thrown)
                            (:definite (ex-data thrown)))
              error (if definite (ex-cause thrown) thrown)]
          (if-let [type (and (instance? SQLException error)
                             (error-type error))]
            (do
              ;; A lost COMMIT reply is indeterminate, never a known abort.
              ;; Reconnect after transport errors to discard session state.
              (when (= :info type) (.close ^Connection @conn))
              (assoc op
                     :type (if definite :fail type)
                     :error [(.getSQLState ^SQLException error)
                             (.getMessage ^SQLException error)]))
            (throw thrown))))))
  (teardown! [this test])
  (close! [this test]
    (when conn (.close ^Connection @conn))))
