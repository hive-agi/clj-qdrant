(ns clj-qdrant.api-integration-test
  "Live-qdrant integration test for api/get-points.

   Activated only when QDRANT_HOST is set. Otherwise every test in this
   namespace is silently skipped so CI stays green in environments
   without a reachable qdrant."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [clj-qdrant.api :as api]
            [clj-qdrant.client :as client]
            [clj-qdrant.schema :as schema]))

(def ^:private qdrant-host (System/getenv "QDRANT_HOST"))

(def ^:private qdrant-port
  (or (some-> (System/getenv "QDRANT_PORT") Integer/parseInt)
      6334))

(def ^:dynamic *conn* nil)

(defn- skip-without-qdrant [f]
  (if (nil? qdrant-host)
    ;; QDRANT_HOST unset — swallow the whole namespace quietly.
    nil
    (let [conn (client/make {:host qdrant-host :port qdrant-port})]
      (try
        (binding [*conn* conn] (f))
        (finally (client/close! conn))))))

(use-fixtures :once skip-without-qdrant)

(defn- fresh-collection-name []
  (str "clj-qdrant-itest-" (java.util.UUID/randomUUID)))

(defn- rand-vec [dim]
  (vec (repeatedly dim #(rand))))

(deftest ^:integration get-points-roundtrip-live
  (testing "upsert 5 points, retrieve by id, verify count + payload roundtrip"
    (let [coll  (fresh-collection-name)
          dim   4
          uuids (repeatedly 5 #(str (java.util.UUID/randomUUID)))
          pts   (mapv (fn [uid i]
                        {:id      uid
                         :vector  (rand-vec dim)
                         :payload {:k "v" :i i}})
                      uuids (range))]
      (try
        (schema/collection-create! *conn* {:name        coll
                                           :vector-size dim
                                           :distance    :cosine})
        (api/upsert-points *conn* :collection coll :points pts)
        ;; qdrant is async-visible; a small retry loop keeps the test
        ;; robust against brief indexing lag.
        (let [result (loop [attempts 10]
                       (let [r (api/get-points *conn*
                                               :collection coll
                                               :ids        uuids)]
                         (if (or (zero? attempts)
                                 (= (count uuids) (count (:points r))))
                           r
                           (do (Thread/sleep 100) (recur (dec attempts))))))
              mapped (mapv api/point->map (:points result))]
          (is (= (count uuids) (count (:points result)))
              "returned point count equals input id count")
          (is (= (set uuids) (set (map :id mapped)))
              "each id roundtrips via point->map")
          (is (every? #(= "v" (get-in % [:payload :k])) mapped)
              ":payload :k roundtrips as \"v\"")
          (is (= (set (range (count uuids)))
                 (set (map #(get-in % [:payload :i]) mapped)))
              ":payload :i roundtrips for every upserted point"))
        (finally
          (try
            (.get (.deleteCollectionAsync (:client *conn*) ^String coll))
            (catch Throwable _ nil)))))))
