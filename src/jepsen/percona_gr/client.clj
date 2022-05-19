(ns jepsen.percona-gr.client
  "Helper functions for interacting with mysql clients."
  (:require [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [with-retry]]
            [jepsen [util :as util]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sql]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (clojure.lang ExceptionInfo)
           (com.mysql.cj.jdbc.exceptions CommunicationsException
                                         MySQLTransactionRollbackException)
           (java.sql Connection
                     SQLException
                     SQLNonTransientConnectionException
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
  "Opens a connection to the given node. Options may override any next.jdbc
  spec options."
  ([test node]
   (open test node {}))
  ([test node opts]
   (let [spec (merge {:dbtype "mysql"
                      :host   node
                      :port   port
                      :user   user
                      :password password
                      :connectTimeout 5000
                      :loginTimeout   5000
                      :socketTimeout  10000}
                     opts)
         ds    (j/get-datasource spec)
         conn  (j/get-connection ds)]
     conn)))

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
  ([test node]
   (await-open test node {}))
  ([test node opts]
   (util/await-fn
     (fn open-and-test []
       (let [conn (open test node opts)]
         (try
           (j/execute-one! conn ["select UUID()"])
           conn
           (catch Throwable t
             (close! conn)
             (throw t)))))
     {:retry-interval 500
      :log-interval   10000
      :log-message    "Waiting for MySQL"})))

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

(defmacro with-rand-aborts
  "Evaluates body, randomly throwing :type :abort on occasion."
  [test & body]
  `(let [res# (do ~@body)]
     (when (< (rand) (:abort-probability ~test))
       (throw+ {:type :abort}))
     res#))

(defmacro with-errors
  "Captures and remaps MySQL errors, returning them as failed/info operations."
  [op & body]
  `(try
     ; A bit weird, but I'm not entirely sure how the
     ; order/pattern-matching/inheritance rules intersect when we do predicate
     ; matches like this mixed with catching normal ExceptionInfos, so we break
     ; them up separately.
     (try+ ~@body
           (catch [:type :abort] e#
             (assoc ~op :type :fail, :error :abort)))

     (catch CommunicationsException e#
       (assoc ~op :type :info, :error [:comms (.getMessage e#)]))

     (catch MySQLTransactionRollbackException e#
       (assoc ~op :type :fail, :error [:rollback (.getMessage e#)]))

     (catch SQLSyntaxErrorException e#
       (condp re-find (.getMessage e#)
         ; This is obviously not a syntax error, what the hell
         #"Unknown database"
         (assoc ~op :type :fail, :error [:unknown-db (.getMessage e#)])

         ; Also obviously not a syntax error
         #"Table '.+' doesn't exist"
         (assoc ~op :type :fail, :error [:table-does-not-exist (.getMessage e#)])

         (throw e#)))

     (catch SQLNonTransientConnectionException e#
       (condp re-find (.getMessage e#)
         ; This explicitly says transaction resolution unknown
         #"Communications link failure during rollback"
         (assoc ~op :type :info, :error [:link-failure-during-rollback
                                         (.getMessage e#)])

         ; Well this can't possibly have committed, so...
         #"No operations allowed after connection closed"
         (assoc ~op :type :fail, :error [:connection-closed (.getMessage e#)])

         (do (warn "Don't know how how to handle SQLNonTransientConnectionException with message" (pr-str (.getMessage e#)))
             (throw e#))))

     (catch SQLException e#
       (condp re-find (.getMessage e#)
         #"Lock deadlock; Retry transaction"
         (assoc ~op :type :fail, :error :deadlock)

         ; This tells us we're talking to a secondary; we'll sleep a bit to
         ; do less ops here. Not *too* much--we still want to frequently
         ; check these nodes so we can see stale reads.
         #"super-read-only"
         (do (Thread/sleep 100)
             (assoc ~op :type :fail, :error [:super-read-only]))

         (throw e#)))

     (catch ExceptionInfo e#
       (info "Caught ExceptionInfo caused by"
             (when-let [c# (.getCause e#)]
               (str (class c#) " " (.getMessage c#))))
       (assoc ~op :type :info, :error [:exception-info (.getMessage e#)]))))

(defn rand-upsert-method
  "Takes a test and returns a randomly chosen method to perform an upsert for a
  test: either :on-dup-key or :update-insert"
  [test]
  (rand-nth
    (cond-> []
      (:update-insert test) (conj :update-insert)
      (:on-dup-key test)    (conj :on-dup-key))))
