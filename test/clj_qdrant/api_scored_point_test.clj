(ns clj-qdrant.api-scored-point-test
  "Pins the search boundary's decode contract.

   `search-points` used to return RAW `Points$ScoredPoint` protobufs and
   never set `with_payload` — qdrant's gRPC default is with_payload=false, so
   even a decoded point carried nothing but an id. Every consumer downstream
   (hive-qdrant store -> carto normalize-semantic -> semantic-grep) then
   dropped the row for lacking :tags/:content, and a populated backend read
   as zero hits.

   These tests build real protobuf points (no live cluster) and assert the
   decoder keeps id, payload AND score."
  (:require [clojure.test :refer [deftest is testing]]
            [clj-qdrant.api :as api])
  (:import [io.qdrant.client PointIdFactory ValueFactory]
           [io.qdrant.client.grpc Points$ScoredPoint]))

(defn- scored-point
  "Build a Points$ScoredPoint with a payload and a score."
  [id score payload]
  (let [b (-> (Points$ScoredPoint/newBuilder)
              (.setId (PointIdFactory/id ^java.util.UUID id))
              (.setScore (float score)))]
    (doseq [[k v] payload]
      (.putPayload b (name k) (ValueFactory/value ^String v)))
    (.build b)))

(deftest scored-point-decode-keeps-payload-and-score
  (testing "scored-point->map yields a clojure map with :id :payload :score"
    (let [u  (java.util.UUID/randomUUID)
          sp (scored-point u 0.42 {:content "(defn foo [] 1)" :project-id "hive-mcp"})
          m  (api/scored-point->map sp)]
      (is (map? m) "must not leak a raw protobuf object")
      (is (= (str u) (:id m)))
      (is (= "(defn foo [] 1)" (get-in m [:payload :content])))
      (is (= "hive-mcp" (get-in m [:payload :project-id])))
      (is (number? (:score m)) "score must survive the decode — an unranked hit is unusable")
      (is (< 0.41 (double (:score m)) 0.43)))))

(deftest scored-point-decode-distinct-scores
  (testing "distinct scores stay distinct (zero-vector regression guard)"
    (let [ms (mapv (fn [s] (api/scored-point->map
                            (scored-point (java.util.UUID/randomUUID) s {:content "x"})))
                   [0.9 0.5 0.1])]
      (is (apply distinct? (map :score ms))))))
