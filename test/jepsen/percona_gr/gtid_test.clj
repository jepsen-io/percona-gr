(ns jepsen.percona-gr.gtid-test
  "Tests for GTID parsing & manipulation"
  (:require [clojure [test :refer :all]]
            [jepsen [util :as util :refer [map-vals]]]
            [jepsen.percona-gr.gtid :as g])
  (:import (com.google.common.collect Range RangeSet)))

(defn ->data
  "Turns a GTID set back into a map of nodes to vectors of [lower upper]
  inclusive ranges."
  [gtid-set]
  (map-vals (fn [^RangeSet range-set]
              (mapv g/range->pair (.asRanges range-set)))
            gtid-set))

(deftest parse-gtid-set-test
  (let [p (comp ->data g/parse-gtid-set)]
    (testing "empty"
      (is (= {}
             (p ""))))

    (testing "single node, single txn"
      (is (= {"foo" [[3 3]]}
             (p "foo:3"))))

    (testing "single node, single range"
      (is (= {"foo" [[2 4]]}
             (p "foo:2-4"))))

    (testing "single node, two ranges"
      (is (= {"foo" [[1 1] [3 5]]}
             (p "foo:3-5:1"))))

    (testing "multiple nodes, multiple ranges"
      (is (= {"foo" [[4 4]]
              "bar" [[5 7] [9 15]]}
             (p "foo:4, bar:5-7:9-15"))))))

(deftest union-test
  (let [u (fn [a b]
            (->data (g/union (g/parse-gtid-set a)
                             (g/parse-gtid-set b))))]
    (testing "merge with self"
      (is (= {"foo" [[1 1] [3 6]]}
             (u "foo:1:3-6" "foo:3-6:1"))))

    (testing "merge with overlapping intervals"
      (is (= {"adjacent"  [[1 10]]
              "overlap"   [[1 10]]
              "contained" [[1 10]]}
             (u "adjacent:1-5, overlap:1-6, contained:1-10"
                "adjacent:6-10, overlap: 4-10, contained:3-5"))))
    ))

(deftest cardinality-test
  (let [c (comp g/cardinality g/parse-gtid-set)]
    (testing "empty"
      (is (= 0 (c ""))))

    (testing "single txn"
      (is (= 1 (c "foo:4"))))

    (testing "multi server, single txns"
      (is (= 3 (c "foo:1, bar:3:6"))))

    (testing "multi server, ranges"
      (is (= 10 (c "foo:1-3:6-10, bar:4:7"))))))

(deftest most-recent-node-test
  (is (= "n2"
         (g/most-recent-node
           ; Weird, but apparently what we're supposed to do
           {"n1" (g/parse-gtid-set "foo:10-12")
            "n2" (g/parse-gtid-set "foo:1-8")}))))
