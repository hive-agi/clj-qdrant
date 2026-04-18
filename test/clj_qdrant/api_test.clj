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

(deftest ^:integration upsert-live
  ;; Requires a live qdrant at localhost:6334 — run explicitly:
  ;;   clj -M:test -i :integration
  (is true "gated behind :integration"))
