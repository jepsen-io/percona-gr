(ns jepsen.percona-gr.gtid
  "Group replication clusters *cannot* be restarted automatically. You, the
  administrator, have to go around to every node and figure out what their set
  of transactions is, parse those sets, and compare them to find 'the most
  up-to-date' replica. Lord help us if that's not a proper superset of the
  others, I suppose.

  This namespace allows us to parse GTID set strings, union them together, and
  compute their cardinality, following https://dev.mysql.com/doc/refman/5.7/en/replication-gtids-concepts.html#replication-gtids-concepts-gtid-sets.

  No, I can't believe this is a real database either.

  We encode GTID Sets as google.collect TreeRangeSets, and use half-open
  ranges to capture the fact that we're dealing with sets of integers, not
  reals. The transaction range 1-5 becomes (0, 5]. This means that merges work
  correctly when we merge 1-5 and 6-10: (0, 5] U (5, 10] = (0, 10]."
  (:require [clojure [set :as set]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [util :refer [parse-long]]])
  (:import (com.google.common.collect Range
                                      RangeSet
                                      TreeRangeSet)))

(defn range->pair
  "Converts an openClosed range (lower - 1, upper] into a closed integer [lower
  upper] pair."
  [^Range r]
  [(inc (.lowerEndpoint r))
   (.upperEndpoint r)])

(defn ^Range ->range
  "Takes an inclusive range of integers from lower to upper, and constructs a
  (lower - 1, upper] range."
  [lower upper]
  (Range/openClosed (dec lower) upper))

(defn parse-txn-ranges
  "Takes a collection of txn ranges like '1-5' or '4', and returns a RangeSet
  encompassing those numbers."
  [txn-ranges]
  (let [range-set (TreeRangeSet/create)]
    (doseq [txn-range txn-ranges]
      (let [[m lower _ upper] (re-find #"(\d+)(-(\d+))?" txn-range)
            _ (when-not m
                (throw (IllegalArgumentException.
                         (str "Expected a txn range, got " txn-range))))
            range (->range (parse-long lower)
                           (parse-long (or upper lower)))]
        (.add range-set range)))
    range-set))

(defn parse-server+txns
  "Takes a string representation of a GTID set from a single server (e.g.
  3E11FA47-71CA-11E1-9E33-C80AA9429562:1-3:11:47-49) and returns a vector of
  [server-id txn-ids], where txn-ids is a RangeSet covering 1-3, 11, and
  47-49."
  [server+txns]
  (let [[server & txn-ranges] (str/split server+txns #":")]
    (when (seq txn-ranges)
      [server (parse-txn-ranges txn-ranges)])))

(defn parse-gtid-set
  "Takes a string representation of a GTID set (e.g.
  2174B383-5441-11E8-B90A-C80AA9429562:1-3, 24DA167-0C0C-11E8-8442-00059A3C7B00:1-19:30-31) and returns a map of servers (UUID strings) to TreeRangeSets of transactions."
  [gtid-set-str]
  (->> (str/split gtid-set-str #",\s+")
       (keep parse-server+txns)
       (into (sorted-map))))

(defn range-set-union
  "Unions two RangeSets"
  [^RangeSet a ^RangeSet b]
  (let [u (TreeRangeSet/create)]
    (.addAll u a)
    (.addAll u b)
    u))

(defn union
  "Combines GTID sets. Identity is an empty map. With one arg, returns that
  GTID set. With two, merges."
  ([]
   {})
  ([a]
   a)
  ([a b]
   (merge-with range-set-union a b)))

(defn cardinality
  "Takes a parsed GTID set (a map of server IDs to RangeSets of transaction
  IDs) and returns the number of transaction IDs in that set."
  [gtid-set]
  (loopr [count 0]
         [[server ranges] gtid-set
          range           (.asRanges ranges)]
         (let [[lower upper] (range->pair range)]
           (recur (+ count 1 (- upper lower))))))

;; TODO: we should probably check to make sure that ranges are actually all
;; subsets of the longest selected set.
(defn most-recent-node
  "Takes a map of nodes to GTID sets and returns the node with the largest
  cardinality GTID set."
  [nodes->gtid-sets]
  (loopr [max-card 0
          max-node nil]
         [[node gtid-set] nodes->gtid-sets]
         (let [card (cardinality gtid-set)]
           (if (< max-card card)
             (recur card node)
             (recur max-card max-node)))
         max-node))
