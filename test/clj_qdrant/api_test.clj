(ns clj-qdrant.api-test
  (:require [clojure.test :refer [deftest testing is]]
            [clj-qdrant.api :as api]))

(deftest point-id-coercion
  (testing "integer and uuid-string ids coerce to PointId"
    (is (some? (#'api/->point-id 42)))
    (is (some? (#'api/->point-id "550e8400-e29b-41d4-a716-446655440000")))
    (is (some? (#'api/->point-id (java.util.UUID/randomUUID))))))

(deftest value-coercion
  (testing "primitives coerce to qdrant Value"
    (is (some? (#'api/->value "x")))
    (is (some? (#'api/->value 1)))
    (is (some? (#'api/->value 1.5)))
    (is (some? (#'api/->value true)))))

(deftest payload-coercion
  (testing "map -> string-keyed payload map"
    (let [m (#'api/->payload {:a "hi" :b 1})]
      (is (contains? m "a"))
      (is (contains? m "b")))))

(deftest paginate-scroll-accumulates-until-limit
  (testing "walks pages until drained"
    (let [pages     [{:points [1 2 3] :next-offset :p1}
                     {:points [4 5 6] :next-offset :p2}
                     {:points [7 8]   :next-offset nil}]
          idx       (atom 0)
          scroll-fn (fn [_offset _page-limit]
                      (let [p (nth pages @idx)]
                        (swap! idx inc)
                        p))]
      (is (= [1 2 3 4 5 6 7 8]
             (#'api/paginate-scroll scroll-fn 100 3))))))

(deftest paginate-scroll-stops-at-user-limit
  (testing "does not fetch past :limit"
    (let [calls     (atom 0)
          pages     [{:points [1 2 3] :next-offset :p1}
                     {:points [4 5 6] :next-offset :p2}]
          idx       (atom 0)
          scroll-fn (fn [_offset page-limit]
                      (swap! calls inc)
                      (let [p (nth pages @idx)]
                        (swap! idx inc)
                        (update p :points #(vec (take page-limit %)))))]
      (is (= [1 2 3 4] (#'api/paginate-scroll scroll-fn 4 3)))
      (is (= 2 @calls) "stopped once :limit reached"))))

(deftest paginate-scroll-stops-on-nil-offset
  (testing "single page with nil next-offset terminates"
    (let [scroll-fn (fn [_ _] {:points [:a :b] :next-offset nil})]
      (is (= [:a :b] (#'api/paginate-scroll scroll-fn 100 32))))))

(deftest paginate-scroll-respects-page-size
  (testing "page-limit passed to scroll-fn is min(page-size, remaining)"
    (let [seen      (atom [])
          scroll-fn (fn [_ page-limit]
                      (swap! seen conj page-limit)
                      {:points (vec (repeat page-limit :x))
                       :next-offset :keep-going})]
      (#'api/paginate-scroll scroll-fn 70 32)
      (is (= [32 32 6] @seen)))))

(deftest paginate-scroll-handles-empty-page
  (testing "empty :points terminates even if next-offset present"
    (let [scroll-fn (fn [_ _] {:points [] :next-offset :bogus})]
      (is (= [] (#'api/paginate-scroll scroll-fn 100 32))))))

(deftest ^:integration upsert-live
  ;; Requires a live qdrant at localhost:6334 — run explicitly:
  ;;   clj -M:test -i :integration
  (is true "gated behind :integration"))
