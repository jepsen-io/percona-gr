(ns jepsen.percona-gr.list-append
  "A basic workload which transactionally reads and appends to lists by primary
  key."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as generator]
                    [util :as util :refer [parse-long]]]
            [jepsen.tests.cycle.append :as append]
            [jepsen.percona-gr [client :as c]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sqlb]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.sql SQLException)))

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

(defn append-using-on-duplicate-key!
  "Appends an element to a key using an INSERT ... ON DUPLICATE KEY UPDATE"
  [conn test table k e]
  (j/execute! conn
    [(str "insert into " table " (id, sk, val) values (?, ?, ?)"
          " on duplicate key update val = CONCAT(val, ',', ?)")
     k k e e]))

(defn mop!
  "Apply a single transaction micro-operation on a connection. Returns the
  completed micro-op."
  [conn test txn? [f k v]]
  (let [table-count (:table-count test)
        table       (table-for table-count k)
        v' (case f
             :r (let [r (j/execute! conn
                                    [(str "select (val) from " table " where "
                                          "id = ?")
                                     k]
                                    {:builder-fn rs/as-unqualified-lower-maps})]
                  (when-let [v (:val (first r))]
                    (mapv parse-long (str/split v #","))))

             :append
             (do (append-using-on-duplicate-key! conn test table k v)
                 v))]
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
                          val text)")]))
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
        ; Juuust in case?
        ;(c/set-transaction-isolation! conn (:isolation test))
        (reset! initialized? true))

      (let [txn      (:value op)
            use-txn? (< 1 (count txn))
            txn'     (if use-txn?
                       (j/with-transaction [t conn
                                            {:isolation (:isolation test)}]
                         (mapv (partial mop! t test true) txn))
                       ; No txn
                       (mapv (partial mop! conn test false) txn))]
        (assoc op :type :ok, :value txn'))))

  (teardown! [_ test])

  (close! [this test]
    (c/close! conn)))

(defn workload
  "A list append workload, given options from the CLI"
  [opts]
  (-> opts
      (assoc :min-txn-length 1
             :consistency-models [(:expected-consistency-model opts)])
      append/test
      (assoc :client (map->Client {}))))
