(ns jepsen.percona-gr
  "Constructs tests and handles CLI arguments."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [string :as str]
                     [pprint :refer [pprint]]]
            [jepsen [db :as jepsen.db]
                    [cli :as cli]
                    [checker :as checker]
                    [generator :as gen]
                    [tests :as tests]
                    [util :as util]]
            [jepsen.os.debian :as debian]
            [jepsen.percona-gr [db :as db]
                               [list-append :as list-append]
                               [nemesis :as nemesis]
                               [rw-register :as rw-register]]))

(def workloads
  "A map of workload names to functions which take CLI options and construct
  partial test maps."
  {:list-append list-append/workload
   :rw-register rw-register/workload})

(def all-workloads
  (keys workloads))

(def nemeses
  "All faults we can perform"
  #{:pause :kill :partition :recover :clock})

(def all-nemeses
  "Combinations of nemeses for tests"
  [[]
   [:partition :recover]
   [:kill :recover]
   [:pause :recover]
   [:pause :kill :partition :clock :recover]])

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:pause :kill :partition :clock :recover]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(def short-isolation
  {:strict-serializable "Strong-1SR"
   :serializable        "S"
   :strong-snapshot-isolation "Strong-SI"
   :snapshot-isolation  "SI"
   :repeatable-read     "RR"
   :read-committed      "RC"
   :read-uncommitted    "RU"})

(defn percona-gr-test
  "Given an options map from the CLI, constructs a test map."
  [opts]
  (let [workload-name (:workload opts)
        workload      ((workloads workload-name) opts)
        db            (if (:no-db opts)
                        jepsen.db/noop
                        (db/db opts))
        nemesis       (nemesis/package
                        {:db        db
                         :nodes     (:nodes opts)
                         :faults    (:nemesis opts)
                         :partition {:targets [:primaries :majority :majorities-ring]}
                         :pause     {:targets [:primaries :majority]}
                         :kill      {:targets [:primaries :majority :all]}
                         :interval         (:nemesis-interval opts)
                         :recover-interval (:recover-interval opts)})
        ]
    (merge tests/noop-test
           opts
           {:name (str (name workload-name)
                       (when (:single-node opts)
                         " single-node")
                       " " (short-isolation (:isolation opts))
                       (when (:predicate-reads opts)
                         " pred")
                       (when-let [sf (:select-for opts)]
                         (str " select-for=" (name sf)))
                       (when (seq (:nemesis opts))
                         (str " " (str/join "," (map name (:nemesis opts))))))
            :os   debian/os
            :db   db
            :checker (checker/compose
                       {:perf       (checker/perf
                                      {:nemeses (:perf nemesis)})
                        :clock      (checker/clock-plot)
                        :stats      (checker/stats)
                        :exceptions (checker/unhandled-exceptions)
                        :workload   (:checker workload)})
            :client    (:client workload)
            :nemesis   (:nemesis nemesis)
            :generator (gen/phases
                         (->> (:generator workload)
                              (gen/stagger (/ (:rate opts)))
                              (gen/nemesis
                                (gen/phases
                                  (gen/sleep 10)
                                  (->> (:generator nemesis)
                                       (gen/time-limit (-> (:time-limit opts)
                                                           (- 10)
                                                           (max 0))))
                                  (gen/log "Ending nemesis for recovery")))
                              (gen/time-limit (-> (:time-limit opts)
                                                  (+ (:recovery-time opts))))))})))

(def cli-opts
  "Additional CLI options"
  [[nil "--abort-probability PROB" "Probability that we decide to abort a transaction just for grins"
    :default 0.1
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]

   [nil "--[no-]on-dup-key" "If true, enables writes using INSERT ... ON DUPLICATE_KEY"
    :default true]

   [nil "--expected-consistency-model MODEL" "What level of isolation do we *expect* to observe? Defaults to the same as --isolation."
    :default :strict-serializable
    :parse-fn keyword]

   [nil "--innodb-flush-method METHOD" "How InnoDB should flush data. Passed directly to mysql's config."
    :default "O_DIRECT"]

   ["-i" "--isolation LEVEL" "What level of isolation we should set: serializable, repeatable-read, etc."
    :default :serializable
    :parse-fn keyword
    :validate [#{:read-uncommitted
                 :read-committed
                 :repeatable-read
                 :serializable}
               "Should be one of read-uncommitted, read-committed, repeatable-read, or serializable"]]

   [nil "--lazyfs" "If set, mounts the data directory in a lazyfs, and drops unfsynced writes on process kill."]

   [nil "--max-txn-length NUM" "Maximum number of operations in a transaction."
    :default  4
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  32
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? nemeses)
               (str (cli/one-of nemeses)
                    " or the special faults all or none.")]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default  10
    :parse-fn read-string
    :validate [pos? "Must be a positive integer."]]

   [nil "--no-db" "Skips DB setup/teardown, running against an existing cluster. Useful for bypassing Percona's incredibly slow cluster setup process while testing."]

   [nil "--[no-]predicate-reads" "If true, selects rows for reads/writes not just via primary key, but also via secondary keys."
    :default true]

   [nil "--table-count NUM" "Number of tables to split rows across."
    :default 2
    :parse-fn parse-long
    :validate [pos? "Must be positive."]]

   ["-r" "--rate HZ" "Approximate number of requests per second, total"
    :default 1000
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]

   [nil "--recover-interval" "How often to recover the cluster during tests."
    :default  10
    :parse-fn parse-long
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]

   [nil "--recovery-time" "How many seconds to wait for the cluster to recover at the end of the test."
    :default  0
    :parse-fn read-string
    :validate [#(and (number? %) (not (neg? %))) "Must be a non-negative number"]]

   [nil "--select-for MODE" "If set, uses SELECT ... FOR UPDATE or FOR SHARE on selects. Using `share` or `update` are applied to every select operation. `share+update` does a `FOR UPDATE` on rows that the transaction will write later, and `FOR SHARE` otherwise."
    :parse-fn #(keyword (.toLowerCase %))
    :validate [#{:share :update :share+update} "Must be either share, update, or share+update. Share and update take out FOR SHARE and FOR UPDATE on every select. Choosing share+update takes out at *least* a FOR SHARE on every select, and a FOR UPDATE when the row will be written later."]]

   [nil "--single-node" "If set, just runs a single Percona node rather than a GR cluster."]

   [nil "--inter-mop-delay MILLIS" "Adds a random exponentially-distributed delay with the given mean in milliseconds, for each micro-op in a transaction."
    :default 20
    :parse-fn read-string
    :validate [#(and (number? %) (not (neg? %))) "Must be a non-negative number"]]

   [nil "--[no-]update-insert" "If true, performs upserts using an update, falling back to an insert, then trying an update again if necessary."
    :default true]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :default  :list-append
    :validate [workloads (cli/one-of workloads)]]
   ])

(defn all-test-options
  "Takes base cli options, a collection of nemeses, workloads, and a test count,
  and constructs a sequence of test options."
  [cli nemeses workloads]
  (for [n nemeses, w workloads, i (range (:test-count cli))]
    (assoc cli
           :nemesis   n
           :workload  w)))

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [test-fn cli]
  (let [nemeses   (if-let [n (:nemesis cli)] [n]  all-nemeses)
        workloads (if-let [w (:workload cli)]
                    [w]
                    all-workloads)]
    (->> (all-test-options cli nemeses workloads)
         (map test-fn))))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  percona-gr-test
                                         :opt-spec cli-opts})
                   (cli/test-all-cmd {:tests-fn (partial all-tests
                                                         percona-gr-test)
                                      :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
