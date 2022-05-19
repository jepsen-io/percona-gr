(ns jepsen.percona-gr.rw-register
  "A basic workload which transactionally performs writes and reads to named
  registers."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as generator]
                    [util :as util :refer [parse-long]]]
            [jepsen.tests.cycle.wr :as wr]
            [jepsen.percona-gr [client :as c]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sqlb]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.sql SQLException
                     SQLIntegrityConstraintViolationException)))

(def db-name
  "The database we use for this test"
  "jepsen_append")

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "txn" i))

(defn table-for
  "What table should we use for the given key?"
  [table-count k]
  (table-name (mod (hash k) table-count)))

(defn write-using-on-duplicate-key!
  "Sets key k to v, or creates k if it does not already exist. Uses INSERT ...
  ON DUPLICATE KEY UPDATE."
  [conn test table k e]
  (j/execute! conn
              [(str "insert into " table " (id, sk, val) values (?, ?, ?)"
                    " on duplicate key update val = ?")
               k k e e]))

(defn insert!
  "Performs an initial insert of a key with initial element e. Catches
  duplicate key exceptions, returning true if succeeded. If the insert fails
  due to a duplicate key, it'll break the rest of the transaction, assuming
  we're in a transaction, so we establish a savepoint before inserting and roll
  back to it on failure."
  [conn test txn? table k e]
  (try
    ;(info (if txn? "" "not") "in transaction")
    (when txn? (j/execute! conn ["savepoint upsert"]))
    (j/execute! conn
                              [(str "insert into " table " (id, sk, val)"
                                    " values (?, ?, ?)")
                               k k (str e)])
    (when txn? (j/execute! conn ["release savepoint upsert"]))
    true
    (catch SQLIntegrityConstraintViolationException e
      (if (re-find #"Duplicate entry" (.getMessage e))
        (do (info (when txn? "txn") "insert failed:" (.getMessage e))
            (when txn? (j/execute! conn ["rollback to savepoint upsert"]))
            false)
        (throw e)))))

(defn update!
  "Performs an update of a key k, setting its value to v. Returns true if the
  update succeeded, false otherwise."
  [conn test table k e]
  (let [res (-> conn
                (j/execute-one!
                  [(str "update " table " set val = ?"
                        " where "
                        (if (and (:predicate-reads test) (< (rand) 0.5))
                          "sk"
                          "id")
                        " = ?")
                   e k]))]
    (-> res :next.jdbc/update-count pos?)))

(defn write-using-update-or-insert!
  "Writes a key, setting it to value using an UPDATE, and if that fails,
  backing off to an INSERT."
  [conn test txn? table k e]
  (or ; Start by updating in-place
      (update! conn test table k e)
      ; Well, that failed--fall back to an insert.
      (insert! conn test txn? table k e)
      ; If that failed we probably raced with another txn--perhaps we're
      ; running at a low isolation level. Try the upsert again, since the row
      ; apparently exists now.
      (update! conn test table k e)
      ; If that failed, uh, ???
      (throw+ {:type    ::homebrew-upsert-failed
               :txn?    txn?
               :table   table
               :key     k
               :element e})))

(defn mop!
  "Apply a single transaction micro-operation on a connection. Returns the
  completed micro-op."
  [conn test txn? [f k v]]
  (let [table-count (:table-count test)
        table       (table-for table-count k)
        v' (case f
             :r (let [r (j/execute! conn
                                    [(str "select (val) from " table " where "
                                          (if (and (:predicate-reads test)
                                                   (< (rand) 0.5))
                                            "sk"
                                            "id")
                                          " = ?")
                                     k]
                                    {:builder-fn rs/as-unqualified-lower-maps})
                      v (:val (first r))]
                  (when v (long v)))

             :w (do (case (c/rand-upsert-method test)
                      :on-dup-key
                      (write-using-on-duplicate-key! conn test table k v)
                      :update-insert
                      (write-using-update-or-insert! conn test txn? table k v))
                    v))]
    (when txn?
      (Thread/sleep (util/rand-exp (:inter-mop-delay test))))
    [f k v']))

; initialized? is an atom which we set when we first use the connection--we set
; up initial isolation levels, logging info, etc. This has to be stateful
; because we don't necessarily know what process is going to use the connection
; at open! time.
(defrecord Client [node conn initialized?]
  client/Client
  (open! [this test node]
    (let [conn (c/open test node)]
      (assoc this :node     node
             :conn          conn
             :initialized?  (atom false))))

  (setup! [_ test]
    (try
      ; Create DB
      (j/execute! conn [(str "create database if not exists " db-name)])
      (j/execute! conn [(str "use " db-name)])
      ; Create tables
      (dotimes [i (:table-count test)]
        (j/execute! conn
                    [(str "create table if not exists " (table-name i)
                          " (id int not null primary key,
                          sk int not null,
                          val int,
                          INDEX (sk, val))")]))
      (catch SQLException e
        (condp re-find (.getMessage e)
          ; We're talking to a secondary that hasn't finished recovery
          ; yet--don't bother. Should succeed on the primary.
          #"super-read-only" nil
          (throw e)))))

  (invoke! [_ test op]
    ; One-time connection setup
    (c/with-errors op
      (when-not @initialized?
        (j/execute! conn [(str "use " db-name)])
        ; For individual ops
        (c/set-transaction-isolation! conn (:isolation test))
        (reset! initialized? true))

      (let [txn      (:value op)
            use-txn? (< 1 (count txn))
            txn'     (if use-txn?
                       (j/with-transaction [t conn
                                            {:isolation (:isolation test)}]
                         (c/with-rand-aborts test
                           (mapv (partial mop! t test true) txn)))
                       ; No txn
                       (mapv (partial mop! conn test false) txn))]
        (assoc op :type :ok, :value txn'))))

  (teardown! [_ test])

  (close! [this test]
    (c/close! conn)))

(defn workload
  "A write-read register workload, given options from the CLI"
  [opts]
  (-> opts
      (assoc :wfr-keys? true
             ;:sequential-keys? true
             :min-txn-length 1
             :consistency-models [(:expected-consistency-model opts)])
      wr/test
      (assoc :client (map->Client {}))))
