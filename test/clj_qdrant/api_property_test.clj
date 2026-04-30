(ns clj-qdrant.api-property-test
  "Generative tests that lock the Qdrant java-interop surface for
   `clj-qdrant.api`. These are pure (no live qdrant), and in particular
   the `get-points` property pins the arity/shape of the
   `QdrantClient.retrieveAsync` call — a regression like the original
   arity-mismatch bug would fail here."
  (:require [clojure.test :refer [deftest is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clj-qdrant.api :as api])
  (:import [io.qdrant.client.grpc Common$PointId]
           [java.util.concurrent CompletableFuture]))

;; ---------------------------------------------------------------------------
;; Property 1: UUID -> ->point-id -> .getUuid roundtrip
;; ---------------------------------------------------------------------------

(def gen-uuid
  "Generator of java.util.UUID values."
  (gen/fmap (fn [_] (java.util.UUID/randomUUID)) gen/nat))

(defspec point-id-uuid-roundtrip 100
  (prop/for-all [u gen-uuid]
    (let [pid (#'api/->point-id u)]
      (and (instance? Common$PointId pid)
           (= (str u) (.getUuid pid))))))

(defspec point-id-uuid-string-roundtrip 100
  (prop/for-all [u gen-uuid]
    (let [s   (str u)
          pid (#'api/->point-id s)]
      (and (instance? Common$PointId pid)
           (= s (.getUuid pid))))))

;; ---------------------------------------------------------------------------
;; Property 2: get-points preserves order & count of ids
;; ---------------------------------------------------------------------------

(definterface IRetrieveStub
  (^java.util.concurrent.Future
    retrieveAsync [^String collection
                   ^java.util.List ids
                   ^boolean withPayload
                   ^boolean withVectors
                   consistency]))

(deftype RetrieveStubClient [captured]
  IRetrieveStub
  (retrieveAsync [_ collection ids _with-payload _with-vectors _consistency]
    (reset! captured {:collection collection :ids (vec ids)})
    (doto (CompletableFuture.) (.complete []))))

(def gen-uuid-vec
  (gen/vector gen-uuid 0 50))

(defspec get-points-preserves-id-count-and-order 50
  (prop/for-all [uuids gen-uuid-vec]
    (let [captured (atom nil)
          stub     (->RetrieveStubClient captured)
          id-strs  (mapv str uuids)]
      (api/get-points {:client stub}
                      :collection "props"
                      :ids id-strs)
      (let [sent (:ids @captured)]
        (and (= (count id-strs) (count sent))
             (every? #(instance? Common$PointId %) sent)
             (= id-strs (mapv #(.getUuid ^Common$PointId %) sent)))))))

;; ---------------------------------------------------------------------------
;; Sanity: defspec registration fired at least one of each property.
;; (defspec itself runs under `clojure.test`, so no extra deftest needed,
;; but we expose a trivial liveness check to make failure messages explicit.)
;; ---------------------------------------------------------------------------

(deftest property-suite-loaded
  (is true "api property namespace loaded"))
