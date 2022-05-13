(ns jepsen.percona-gr.client
  "Helper functions for interacting with mysql clients."
  (:require [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [with-retry]]
            [jepsen [util :as util]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sql]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (com.mysql.cj.jdbc.exceptions CommunicationsException
                                         MySQLTransactionRollbackException)
           (java.sql Connection
                     SQLException
                     SQLSyntaxErrorException)))

(def port
  "MySQL port"
  3306)

(def replica-port
  "Port for inter-replica traffic"
  33061)

(def user
  "MySQL username"
  "jepsen")

(def password
  "MySQL user password."
  "jepsenpw")

(defn open
  "Opens a connection to the given node."
  [test node]
  (let [spec {:dbtype "mysql"
              :host   node
              :port   port
              :user   user
              :password password}
        ds    (j/get-datasource spec)
        conn  (j/get-connection ds)]
    conn))

(defn close!
  "Closes a connection."
  [^Connection conn]
  (.close conn))

(defn await-table-creatable
  "Waits for a connection to allow table creation."
  [conn]
  (util/await-fn
    (fn create-table []
      (j/execute-one! conn ["create database if not exists jepsen_await"])
      (j/execute-one! conn ["use jepsen_await"])
      (j/execute-one! conn ["create table if not exists jepsen_await (id int auto_increment primary key)"]))
    {:retry-interval 500
     :log-interval 10000
     :log-message "Waiting for tables to be creatable"})
  conn)

(defn await-open
  "Waits for a connection to a node to become available, returning conn.
  Helpful for starting up."
  [test node]
  (util/await-fn
    (fn open-and-test []
      (let [conn (open test node)]
        (try
          (j/execute-one! conn ["select UUID()"])
          conn
          (catch Throwable t
            (close! conn)
            (throw t)))))
    {:retry-interval 500
     :log-interval   10000
     :log-message    "Waiting for MySQL"}))

(defn set-transaction-isolation!
  "Sets the transaction isolation level on a connection. Returns conn."
  [conn level]
  (.setTransactionIsolation
    conn
    (case level
      :serializable     Connection/TRANSACTION_SERIALIZABLE
      :repeatable-read  Connection/TRANSACTION_REPEATABLE_READ
      :read-committed   Connection/TRANSACTION_READ_COMMITTED
      :read-uncommitted Connection/TRANSACTION_READ_UNCOMMITTED))
  conn)

(defmacro with-errors
  "Captures and remaps MySQL errors, returning them as failed/info operations."
  [op & body]
  `(try ~@body
        (catch MySQLTransactionRollbackException e#
          (assoc ~op :type :fail, :error [:rollback (.getMessage e#)]))

        (catch SQLSyntaxErrorException e#
          (condp re-find (.getMessage e#)
            ; This is obviously not a syntax error, what the hell
            #"Unknown database"
            (assoc ~op :type :fail, :error [:unknown-db (.getMessage e#)])

            (throw e#)))

        (catch SQLException e#
          (condp re-find (.getMessage e#)
            #"Lock deadlock; Retry transaction"
            (assoc ~op :type :fail, :error :deadlock)

            #"super-read-only"
            (assoc ~op :type :fail, :error [:super-read-only (.getMessage e#)])

            (throw e#)))))
