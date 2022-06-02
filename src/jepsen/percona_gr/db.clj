(ns jepsen.percona-gr.db
  "Installs and configures Percona Server with group replication."
  (:require [clojure [pprint :refer [pprint]]
                     [set :as set]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [real-pmap]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [lazyfs :as lazyfs]
                    [util :as util :refer [pprint-str]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [jepsen.percona-gr [client :as client :refer [port
                                                          replica-port
                                                          user
                                                          password]]
                               [gtid :as gtid]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sql]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (com.mysql.cj.jdbc.exceptions CommunicationsException)
           (java.util.concurrent Semaphore)))

(def data-dir
  "Where does MySQL store data?"
  "/var/lib/mysql")

(def os-user
  "What linux username owns mysql files?"
  "mysql")

(def expected-version
  "This is sort of fragile, but we expect this version to get installed from
  Percona's servers, and check for it to skip the reinstallation process."
  "8.0.27-18-1.bullseye")

(defn server-id
  "Returns the server ID for a specific node in a test."
  [node test]
  (inc (.indexOf (:nodes test) node)))

(defn install-percona!
  "Installs Percona from Debian repos, following
  https://www.percona.com/doc/percona-server/LATEST/installation/apt_repo.html"
  [test]
  (let [v (debian/installed-version "percona-server-server")]
    (info "Percona server version" v)
    ; TODO: make this version check less fragile--we actually install whatever
    ; the latest rev is.
    (when-not (= v expected-version)
      (info "Installing Percona")
      (c/su
        (debian/install [:gnupg2 :curl :lsb-release])
        ; Install percona repos
        (let [tmpdir "/tmp/jepsen/percona-dl"]
          (c/exec :mkdir :-p tmpdir)
          (c/cd tmpdir
                (c/exec :wget (c/lit "https://repo.percona.com/apt/percona-release_latest.$(lsb_release -sc)_all.deb"))
                (c/exec :dpkg :-i (c/lit "percona-release_latest.$(lsb_release -sc)_all.deb")))
          (c/exec :rm :-rf tmpdir))
        ; Enable repository
        (c/exec :percona-release :setup :ps80)
        ; And install server
        (debian/install ["percona-server-server"])))))

(defn create-user!
  "Creates a MySQL admin user on the local node."
  []
  (info "Creating MySQL admin user")
  (c/su
    (-> {:cmd "mysql"
         :in (str "SET SQL_LOG_BIN=0;\n
                  create user '" user "'@'%' identified by '" password "';\n"
                  "grant all on *.* to " user " with grant option;\n"
                  "SET SQL_LOG_BIN=1;\n")}
        c/wrap-cd
        c/wrap-sudo
        c/ssh*
        jepsen.control.core/throw-on-nonzero-exit)))

(defn configure-common!
  "Writes config files for the server for both GR and single-node deployments."
  [test node opts]
  (info "Writing common config file")
  (c/su
    (-> (io/resource "common.cnf")
        slurp
        (str/replace #"%INNODB_FLUSH_METHOD%" (:innodb-flush-method test))
        (cu/write-file! "/etc/mysql/conf.d/common.cnf"))))

(defn configure-gr!
  "Writes config files for the server for GR deployments."
  [test node opts]
  (info "Writing GR config file")
  (c/su
    (-> (io/resource "gr.cnf")
        slurp
        (str/replace #"%SERVER_ID%" (str (server-id node test)))
        (str/replace #"%GR_LOCAL_ADDRESS%" (str (cn/ip node) ":" replica-port))
        (str/replace #"%GR_SEEDS%" (->> (:seeds opts)
                                        (remove #{node})
                                        (map (fn [seed]
                                               (str (cn/ip seed) ":"
                                                    replica-port)))
                                        (str/join ",")))
        (str/replace #"%GR_START_ON_BOOT%" (if (:start-on-boot? opts)
                                             "ON"
                                             "OFF"))
        (str/replace #"%SUPER_READ_ONLY%" (if (:super-read-only? opts)
                                            "ON"
                                            "OFF"))
        (cu/write-file! "/etc/mysql/conf.d/gr.cnf"))))

(defn configure!
  "Writes config files for the server. Options are:

  :seeds            List of nodes to connect to as seeds. Local node will be
                    filtered out of this list.
  :start-on-boot?   true/false
  :super-read-only? true/false"
  [test node opts]
  (configure-common! test node opts)
  (when-not (:single-node test)
    (configure-gr! test node opts)))

(defn create-replication-user!
  "Creates a replication user on each node."
  [conn]
  (info "Creating replication user")
  (j/execute-one! conn ["SET SQL_LOG_BIN=0"])
  ; If we let mysql use the default sha2 auth, it'll require TLS conns and I'm
  ; not opening that box of worms right now
  (j/execute-one! conn ["CREATE USER replica@'%' IDENTIFIED WITH mysql_native_password BY 'replicapw'"])
  (j/execute-one! conn ["GRANT REPLICATION SLAVE ON *.* TO replica@'%'"])
  (j/execute-one! conn ["GRANT CONNECTION_ADMIN ON *.* TO replica@'%'"])
  (j/execute-one! conn ["GRANT BACKUP_ADMIN ON *.* TO replica@'%'"])
  (j/execute-one! conn ["GRANT GROUP_REPLICATION_STREAM ON *.* TO replica@'%'"])
  (j/execute-one! conn ["FLUSH PRIVILEGES"])
  (j/execute-one! conn ["SET SQL_LOG_BIN=1"]))

(defn set-replication-source!
  "Per
  https://dev.mysql.com/doc/refman/8.0/en/group-replication-user-credentials.html,
  sets the credentials for the server to use as a part of distributed
  recovery."
  [conn]
  (info "Setting replication source")
  (j/execute-one! conn ["CHANGE REPLICATION SOURCE TO SOURCE_USER='replica', SOURCE_PASSWORD='replicapw' FOR CHANNEL 'group_replication_recovery'"]))

(defn stop-group-replication!
  "Stops the group replication system."
  [conn]
  (info "Stopping group replication")
  (j/execute-one! conn ["STOP GROUP_REPLICATION"]))

(defn start-group-replication!
  "Starts the group replication system."
  [conn]
  (info "Starting group replication")
  (j/execute-one! conn ["START GROUP_REPLICATION"]))

(defn bootstrap-group!
  "Starts group replication and bootstraps the group--should be run only on the
  primary."
  [conn]
  (info "Bootstrapping group replication")
  (j/execute-one! conn ["SET GLOBAL group_replication_bootstrap_group=ON"])
  (start-group-replication! conn)
  (j/execute-one! conn ["SET GLOBAL group_replication_bootstrap_group=OFF"]))

(defn restart!
  "Restarts Percona"
  []
  (info "Restarting Percona")
  (c/su (c/exec :service :mysql :restart)))

(defn gtid-executed
  "Returns the GTID set from this connection's GTID_EXECUTED set."
  [conn]
  (->> (j/execute! conn ["SELECT @@GLOBAL.GTID_EXECUTED"]
                   {:builder-fn rs/as-arrays})
       next
       (map first)
       (map gtid/parse-gtid-set)
       (reduce gtid/union)))

(defn certified-txns
  "Returns the GTID set of certified transactions on the group replication
  channel."
  [conn]
  (->> (j/execute! conn ["SELECT received_transaction_set FROM performance_schema.replication_connection_status WHERE channel_name=\"group_replication_applier\""]
                   {:builder-fn rs/as-unqualified-maps})
       (map :received_transaction_set)
       (map gtid/parse-gtid-set)
       (reduce gtid/union)))

(defn members
  "Returns the group replication members table."
  [conn]
  (->> (j/execute! conn ["SELECT * FROM performance_schema.replication_group_members"]
                   {:builder-fn rs/as-unqualified-lower-maps})))

(defn primaries
  "Returns the nodes we think are primaries on this connection."
  [conn]
  (->> (j/execute! conn ["SELECT MEMBER_HOST FROM performance_schema.replication_group_members WHERE MEMBER_ROLE = 'PRIMARY'"]
                  {:builder-fn rs/as-unqualified-lower-maps})
       (map :member_host)
       set))

(defn recover-cluster!
  "MySQL group replication literally can't be restarted automatically. You have
  to log in to every node, extract GTID data and certified transactions,
  compare them to find 'the biggest set' (lol that sounds safe) and then
  re-bootstrap the entire replication group using that node as a primary.

  Invoke this function only in one thread; it'll parallelize over all nodes.

  I cannot believe this is considered acceptable for a distributed database in
  TYOOL 2022:
  https://dev.mysql.com/doc/refman/8.0/en/group-replication-restarting-group.html.

  This process is incredibly slow--like 5+ minutes sometimes--and I can't
  figure out how to make it faster yet. :("
  [test]
  (locking recover-cluster!
    (info "Attempting to recover cluster")
    ; Get a connection to each node
    (let [conns (->> (:nodes test)
                     (map (fn [node]
                            (let [conn (client/await-open
                                         test node
                                         ; Shutting down GR is going to take
                                         ; FOREVER
                                         {:socketTimeout 1000000})]
                              (info "Connection to" node "established")
                              [node conn])))
                     (into {}))
          _ (info "Connections established")
          ; First, ensure GR is stopped, and collect txn sets from each node.
          txns-by-node (->> conns
                            (pmap (fn [[node conn]]
                                    (util/with-thread-name (str "recover " node)
                                      (stop-group-replication! conn)
                                      (info "Fetching GTIDs...")
                                      (let [gtids (gtid/union
                                                    (gtid-executed conn)
                                                    (certified-txns conn))]
                                        (info "GTIDs fetched")
                                        [node gtids]))))
                            (into (sorted-map)))
          _ (info :txns-by-node "\n" (with-out-str (pprint txns-by-node)))

          ; The new primary will be the node with the most txns in its GTID set
          primary (gtid/most-recent-node txns-by-node)
          _ (info :new-primary primary)
          primary-conn (get conns primary)

          ; Now bootstrap that primary
          _ (bootstrap-group! primary-conn)
          _ (info "Bootstrapped members:\n" (pprint-str (members primary-conn)))

          ; And join secondaries
          _ (->> conns
                 (remove (comp #{primary} key))
                 (mapv (fn [[node conn]]
                         (info "Joining" node)
                         (start-group-replication! conn))))
          _ (info "Final members:\n" (pprint-str (members primary-conn)))]
      )))

(defrecord DB [restart-permits needs-recovery?]
  db/DB
  (setup! [this test node]
    ; Lazily initialize restart permits
    (deliver restart-permits
             (Semaphore. (-> (:nodes test)
                             count
                             util/majority
                             (- 2)
                             (max 1))))
    (reset! needs-recovery? true)

    (install-percona! test)
    (db/start! this test node)
    ; When we first launch, we need the only configured seed to be the primary
    ; node--if we allow it to see a node that's not running, it'll kill itself.
    ; <sigh>.
    (configure! test node {:seeds [(jepsen/primary test)]})
    (restart!)
    (create-user!)
    (jepsen/synchronize test)

    ; Set up replication
    (with-open [conn (client/await-open test node)]
      (create-replication-user! conn)
      (set-replication-source! conn)
      (jepsen/synchronize test)

      ; Bootstrap primary
      (when (= node (jepsen/primary test))
        (bootstrap-group! conn))
      (jepsen/synchronize test)

      ; Join secondaries
      ;
      ; Per
      ; https://dev.mysql.com/doc/refman/8.0/en/group-replication-configuring-instances.html,
      ; "Creating a group and joining multiple members at the same time is not
      ; supported. It might work, but chances are that the operations race and
      ; then the act of joining the group ends up in an error or a time out."
      (locking this
        (when (not= node (jepsen/primary test))
          (start-group-replication! conn)))
      (jepsen/synchronize test))

    ; Now we need to configure *again* to enable GR on boot and turn on
    ; super_read_only, which prevents writes to nodes which don't start GR
    ; for whatever reason.
    ;
    ; I think we also have to set *all* the nodes as seeds, because if nodes
    ; restart and they can't find anyone else they kill themselves??? This is
    ; one of the most confusing cluster join processes I've ever used.
    (configure! test node {:start-on-boot? true
                           :super-read-only? true
                           :seeds (:nodes test)})
    ; If we restart nodes concurrently the cluster will tear itself apart and
    ; we'll have to go through an awful, slow recovery process, so instead we
    ; try to stagger the restarts.
    (.acquire @restart-permits)
    (try
      (restart!)
      (client/close! (client/await-open test node))
      (finally
        (.release @restart-permits)))
    (jepsen/synchronize test)

    (with-open [conn (client/open test node)]
      (let [primaries (primaries conn)]
        (info "Primaries are" (pr-str primaries))
        (when (seq primaries)
          ; At least some node thinks there's a primary!
          (reset! needs-recovery? false))))
    (jepsen/synchronize test)

    (when (= node (jepsen/primary test))
      (when @needs-recovery?
        (info "No primaries left; graceful restart must have broken the cluster. Going to do a slow recovery.")
        (recover-cluster! test))))

  (teardown! [this test node]
    (c/su
      (db/kill! this test node)
      (c/exec :rm :-rf
              (c/lit (str data-dir "/*"))
              "/etc/mysql/conf.d/common.cnf"
              "/etc/mysql/conf.d/gr.cnf"
              (c/lit "/var/log/mysql/*"))))

  db/LogFiles
  (log-files [this test node]
    {"/var/log/mysql/error.log" "error.log"})

  db/Process
  (start! [this test node]
    (c/su
      (c/exec :service :mysql :start)))

  (kill! [this test node]
    (c/su
      (cu/grepkill! :mysql)
      (c/exec :service :mysql :stop)))

  db/Pause
  (pause! [this test node]
    (c/su (cu/grepkill! :stop :mysql)))

  (resume! [this test node]
    (c/su (cu/grepkill! :cont :mysql)))

  db/Primary
  (setup-primary! [_ test node])

  (primaries [db test]
    (->> (:nodes test)
         (real-pmap (fn [node]
                      (try
                        (with-open [conn (client/open test node)]
                          (primaries conn))
                        (catch CommunicationsException e
                          nil))))
         (reduce set/union))))

(defrecord SingleNodeDB []
  db/DB
  (setup! [this test node]
    (assert (= 1 (count (:nodes test))))
    (install-percona! test)
    (configure! test node {})
    (db/start! this test node)
    (create-user!))

  (teardown! [this test node]
    (c/su
      (db/kill! this test node)
      (c/exec :rm :-rf
              (c/lit (str data-dir "/*"))
              "/etc/mysql/conf.d/common.cnf"
              "/etc/mysql/conf.d/gr.cnf"
              (c/lit "/var/log/mysql/*"))))

  db/LogFiles
  (log-files [this test node]
    {"/var/log/mysql/error.log" "error.log"})

  db/Process
  (start! [this test node]
    (c/su
      (c/exec :service :mysql :start)))

  (kill! [this test node]
    (c/su
      (cu/grepkill! :mysql)
      (c/exec :service :mysql :stop)))

  db/Pause
  (pause! [this test node]
    (c/su (cu/grepkill! :stop :mysql)))

  (resume! [this test node]
    (c/su (cu/grepkill! :cont :mysql)))

  db/Primary
  (setup-primary! [_ test node])

  (primaries [db test]
    (:nodes test)))

(defrecord LazyFSDB [lazyfs percona]
  db/DB
  (setup! [_ test node]
    ; So we have the right user
    (install-percona! test)
    (db/setup! lazyfs test node)
    (db/setup! percona test node)
    ; I've seen some really weird errors around e.g. empty RSA key files which
    ; make me think maybe we aren't even getting the initial data synced, so
    ; let's force that here.
    (lazyfs/checkpoint! lazyfs))

  (teardown! [_ test node]
    (db/teardown! percona test node)
    (db/teardown! lazyfs test node))

  db/Primary
  (setup-primary! [_ test node]
    (db/setup-primary! percona test node))

  (primaries [_ test]
    (db/primaries percona test))

  db/LogFiles
  (log-files [_ test node]
    (merge (db/log-files percona test node)
           (db/log-files lazyfs test node)))

  db/Process
  (start! [_ test node]
    (db/start! percona test node))

  (kill! [_ test node]
    (db/kill! percona test node)
    (lazyfs/lose-unfsynced-writes! lazyfs))

  db/Pause
  (pause! [_ test node]
    (db/pause! percona test node))

  (resume! [_ test node]
    (db/resume! percona test node)))

(defn lazyfs-db
  "Wraps a Percona database, making sure its data directory is a lazyfs mount."
  [percona]
  (LazyFSDB. (lazyfs/db {:dir  data-dir
                         :user os-user})
             percona))

(defn db
  "Constructs a new DB given options from the CLI. Options:

  :single-node    If set, just runs a single node."
  [opts]
  (cond-> (if (:single-node opts)
            (SingleNodeDB.)
            (DB. (promise) (atom false)))
    (:lazyfs opts) lazyfs-db))
