(ns clj-qdrant.schema-test
  (:require [clojure.test :refer [deftest testing is]]
            [clj-qdrant.schema :as s]))

(deftest distance-lookup
  (testing "keyword resolves to Distance enum"
    (is (some? (s/->distance :cosine)))
    (is (some? (s/->distance :euclid)))
    (is (some? (s/->distance :dot))))
  (testing "unknown keyword throws"
    (is (thrown? clojure.lang.ExceptionInfo (s/->distance :bogus)))))

(deftest vectors-config-builds
  (testing "returns non-nil VectorsConfig"
    (is (some? (s/vectors-config {:size 384 :distance :cosine})))))

(deftest hnsw-config-builds
  (testing "builds with partial opts"
    (is (some? (s/hnsw-config {:m 16 :ef-construct 256})))
    (is (some? (s/hnsw-config {})))))

(deftest payload-schema-types-present
  (is (every? #(contains? s/payload-schema-types %)
              [:keyword :integer :float :geo :text :bool])))
