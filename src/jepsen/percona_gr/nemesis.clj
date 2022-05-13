(ns jepsen.percona-gr.nemesis
  "Nemeses for Percona GR"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [real-pmap]]
            [jepsen [control :as c]
                    [db :as jepsen.db]
                    [generator :as gen]
                    [nemesis :as n]
                    [net :as net]
                    [util :as util]]
            [jepsen.nemesis [combined :as nc]
                            [time :as nt]]
            [jepsen.percona-gr [db :as db]]))

(defn recover-nemesis
  "A nemesis which recovers the cluster."
  [db]
  (reify
    n/Reflection
    (fs [this] [:recover])

    n/Nemesis
    (setup! [this test]
      this)

    (invoke! [this test op]
      (case (:f op)
        :recover
        (let [primaries (jepsen.db/primaries db test)]
          (if (seq primaries)
            (do (info "Cluster has primaries" primaries "so not recovering.")
                (assoc op :value :no-need))
            (do ; First, nodes need to be running and resumed, or this won't
                ; work
                (info "Restarting any stopped nodes")
                (c/on-nodes test (partial jepsen.db/start! db))
                (info "Resuming any paused nodes")
                (c/on-nodes test (partial jepsen.db/resume! db))
                ; Now we can recover
                (db/recover-cluster! test)
                (assoc op :value :recovered))))))

    (teardown! [this test])))

(defn recover-package
  "Periodically recovers the cluster, so we can stress it harder."
  [opts]
  (let [needed? ((:faults opts) :recover)
        db      (:db opts)
        gen     (->> (repeat {:type :info, :f :recover})
                     (gen/stagger (:recover-interval opts)))]
    {:generator       (when needed? gen)
     :final-generator {:type :info, :f :recover}
     :nemesis         (recover-nemesis db)
     :perf            #{{:name "recover"
                         :fs   #{:recover}
                         :color "#80E87F"}}}))

(defn package
  "Constructs a nemesis and generators for Percona GR from nemesis package
  opts. Extra options include:

    :recover-interval - How frequently to schedule recovery of the cluster"
  [opts]
  (let [opts  (update opts :faults set)
        pkgs  (concat (nc/nemesis-packages opts)
                      [(recover-package opts)])
        pkg   (nc/compose-packages pkgs)
        ; Rewrite performance checker information so it knows that a recover op
        ; terminates kill/pause ops. This... exposes a bug, I think, in the
        ; nemesis bar rendering system, so let's try this again later.
        ;perf  (->> (:perf pkg)
        ;           (map (fn [perf]
        ;                  (info (with-out-str (pprint perf)))
        ;                  (if (some #{:kill :pause} (:start perf))
        ;                    (update perf :stop conj :recover)
        ;                    perf)))
        ;           set)
        ;_ (info (with-out-str (pprint perf)))
        ;pkg  (assoc pkg :perf perf)
        ]
    pkg))
