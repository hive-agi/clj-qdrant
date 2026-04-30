(ns clj-qdrant.api-test
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.reflect :as reflect]
            [clj-qdrant.api :as api])
  (:import [io.qdrant.client.grpc Common$PointId]
           [java.util.concurrent CompletableFuture]))

(deftest point-id-coercion
  (testing "integer and uuid-string ids coerce to PointId"
    (is (some? (#'api/->point-id 42)))
    (is (some? (#'api/->point-id "550e8400-e29b-41d4-a716-446655440000")))
    (is (some? (#'api/->point-id (java.util.UUID/randomUUID))))))

(deftest point-id-roundtrip
  (testing "uuid roundtrip: UUID instance -> PointId -> .getUuid equals original"
    (let [u   (java.util.UUID/randomUUID)
          pid (#'api/->point-id u)]
      (is (instance? Common$PointId pid))
      (is (= (str u) (.getUuid pid)))))
  (testing "uuid-string roundtrip: String -> PointId -> .getUuid equals original"
    (let [s   "550e8400-e29b-41d4-a716-446655440000"
          pid (#'api/->point-id s)]
      (is (instance? Common$PointId pid))
      (is (= s (.getUuid pid)))))
  (testing "integer roundtrip: Long -> PointId -> .getNum equals original"
    (let [pid (#'api/->point-id 42)]
      (is (instance? Common$PointId pid))
      (is (= 42 (.getNum pid))))))

;; ---------------------------------------------------------------------------
;; Reflection guard for the get-points retrieveAsync overload.
;; This test fails loudly if the SDK ever drops the 5-arg overload
;; `(String, List, boolean, boolean, ReadConsistency)` that api/get-points
;; currently targets.
;; ---------------------------------------------------------------------------

(defn- retrieve-async-overloads
  "Return set of parameter-type vectors for all `retrieveAsync` methods
   exposed on io.qdrant.client.QdrantClient."
  []
  (->> (:members (reflect/reflect io.qdrant.client.QdrantClient))
       (filter #(= 'retrieveAsync (:name %)))
       (map :parameter-types)
       (map vec)
       set))

(deftest qdrant-client-has-expected-retrieve-async-overload
  (testing "io.qdrant.client.QdrantClient exposes the 5-arg retrieveAsync
            (String, List, boolean, boolean, ReadConsistency) overload
            that api/get-points targets"
    (let [expected '[java.lang.String
                     java.util.List
                     boolean
                     boolean
                     io.qdrant.client.grpc.Points$ReadConsistency]]
      (is (contains? (retrieve-async-overloads) expected)
          (str "api/get-points calls retrieveAsync with a signature that "
               "must match a real overload. Seen: "
               (retrieve-async-overloads))))))

(definterface IRetrieveStub
  (^java.util.concurrent.Future
    retrieveAsync [^String collection
                   ^java.util.List ids
                   ^boolean withPayload
                   ^boolean withVectors
                   consistency]))

(deftype RetrieveStubClient [captured]
  IRetrieveStub
  (retrieveAsync [_ collection ids with-payload with-vectors consistency]
    (reset! captured {:collection   collection
                      :ids          (vec ids)
                      :with-payload with-payload
                      :with-vectors with-vectors
                      :consistency  consistency})
    (doto (CompletableFuture.) (.complete []))))

(deftest get-points-invokes-retrieve-async-with-live-overload
  (testing "get-points does not throw arity-mismatch against a client that
            implements only the 5-arg retrieveAsync overload"
    (let [captured (atom nil)
          stub     (->RetrieveStubClient captured)
          result   (api/get-points {:client stub}
                                   :collection "c"
                                   :ids [1 2 3])]
      (is (= "c" (:collection result)))
      (is (= [] (:points result)))
      (is (= "c" (:collection @captured)))
      (is (= 3 (count (:ids @captured))))
      (is (every? #(instance? Common$PointId %) (:ids @captured)))
      (is (true? (:with-payload @captured)))
      (is (true? (:with-vectors @captured)))
      (is (nil? (:consistency @captured))))))

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

(deftest with-payload-selector-enable-when-empty
  (testing "nil/empty payload-includes -> enable=true selector (full payload)"
    (let [sel (#'api/->with-payload-selector nil)]
      (is (true? (.getEnable sel)))
      (is (false? (.hasInclude sel))))
    (let [sel (#'api/->with-payload-selector [])]
      (is (true? (.getEnable sel)))
      (is (false? (.hasInclude sel))))))

(deftest with-payload-selector-include-projection
  (testing "non-empty payload-includes -> PayloadIncludeSelector with fields"
    (let [sel    (#'api/->with-payload-selector ["type" :tags "project-id"])
          fields (vec (.. sel getInclude getFieldsList))]
      (is (true? (.hasInclude sel)))
      (is (= ["type" "tags" "project-id"] fields))
      (is (false? (.getEnable sel))
          "enable oneof cleared when include set"))))

(deftest ^:integration upsert-live
  ;; Requires a live qdrant at localhost:6334 — run explicitly:
  ;;   clj -M:test -i :integration
  (is true "gated behind :integration"))
