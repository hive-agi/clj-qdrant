(ns clj-qdrant.api
  "Point CRUD + search over the official qdrant java client.

   All functions take a client-map (from `clj-qdrant.client/make`) and
   kw-args. Return values are Clojure maps summarizing the operation —
   raw java responses are stashed under :results / :points when useful."
  (:import [io.qdrant.client ConditionFactory PointIdFactory ValueFactory VectorsFactory]
           [io.qdrant.client.grpc Common$Filter
                                  JsonWithInt$Value
                                  Points$PointStruct
                                  Points$ScrollPoints
                                  Points$SearchPoints
                                  Points$WithPayloadSelector]))

;; ---------------------------------------------------------------------------
;; Coercions
;; ---------------------------------------------------------------------------

(defn- ->point-id [v]
  (cond
    (integer? v)         (PointIdFactory/id (long v))
    (instance? java.util.UUID v) (PointIdFactory/id ^java.util.UUID v)
    (string? v)          (PointIdFactory/id (java.util.UUID/fromString v))
    :else                v))

(declare ->value)

(defn- ->value-list
  "Wrap a clojure seq as a qdrant ListValue, recursively converting each element."
  [xs]
  (ValueFactory/list (mapv ->value xs)))

(defn- ->value [v]
  (cond
    (nil? v)               (ValueFactory/nullValue)
    (string? v)            (ValueFactory/value ^String v)
    (integer? v)           (ValueFactory/value (long v))
    (double? v)            (ValueFactory/value (double v))
    (boolean? v)           (ValueFactory/value (boolean v))
    (sequential? v)        (->value-list v)
    (instance? java.util.List v) (->value-list (vec v))
    :else                  (ValueFactory/value (str v))))

(defn- ->payload [m]
  (reduce-kv (fn [acc k v] (assoc acc (name k) (->value v))) {} m))

(defn- ->point [{:keys [id vector payload]}]
  (let [b (-> (Points$PointStruct/newBuilder)
              (.setId (->point-id id))
              (.setVectors (VectorsFactory/vectors (float-array vector))))]
    (when (seq payload) (.putAllPayload b (->payload payload)))
    (.build b)))

;; ---------------------------------------------------------------------------
;; CRUD
;; ---------------------------------------------------------------------------

(defn upsert-points
  "Upsert points into a collection.

   kw-args: :collection :points [:wait?]
   points are maps with :id :vector [:payload]."
  [{:keys [client]} & {:keys [collection points]}]
  (let [structs (mapv ->point points)]
    (.get (.upsertAsync client ^String collection structs))
    {:collection collection
     :count      (count points)
     :operation  :upsert}))

(defn search-points
  "Vector similarity search.

   kw-args: :collection :vector :limit [:with-payload?]"
  [{:keys [client]} & {:keys [collection vector limit]
                       :or   {limit 10}}]
  (let [req (-> (Points$SearchPoints/newBuilder)
                (.setCollectionName ^String collection)
                (.addAllVector (map float vector))
                (.setLimit (long limit))
                .build)
        res (.get (.searchAsync client req))]
    {:collection collection
     :count      (count res)
     :results    (vec res)}))

(defn delete-points
  "Delete points by id list.

   kw-args: :collection :ids"
  [{:keys [client]} & {:keys [collection ids]}]
  (.get (.deleteAsync client ^String collection (mapv ->point-id ids)))
  {:collection collection
   :deleted    (count ids)
   :operation  :delete})

(defn get-points
  "Retrieve points by id list.

   kw-args: :collection :ids"
  [{:keys [client]} & {:keys [collection ids]}]
  (let [res (.get (.retrieveAsync client
                                  ^String collection
                                  (mapv ->point-id ids)
                                  true true))]
    {:collection collection
     :points     (vec res)}))

(defn ->filter
  "Build a Common$Filter from a map of must-conditions.

   opts:
     :must-keyword {field-name [string ...]}  — match-any-keyword per field
     :must-keywords-all [{:field f :value v}] — AND of single-keyword matches

   Returns a Common$Filter or nil if no conditions provided."
  [{:keys [must-keyword must-keywords-all]}]
  (let [b (Common$Filter/newBuilder)
        any-added? (atom false)]
    (doseq [[field values] must-keyword
            :when (seq values)]
      ;; matchKeywords is OR over the list — for AND-of-tags we add one Condition per tag.
      (doseq [v values]
        (.addMust b (ConditionFactory/matchKeyword (name field) ^String v))
        (reset! any-added? true)))
    (doseq [{:keys [field value]} must-keywords-all]
      (.addMust b (ConditionFactory/matchKeyword (name field) ^String value))
      (reset! any-added? true))
    (when @any-added? (.build b))))

(defn- value->clj
  "Decode a JsonWithInt$Value into a clojure value."
  [^JsonWithInt$Value v]
  (cond
    (.hasNullValue v)   nil
    (.hasStringValue v) (.getStringValue v)
    (.hasIntegerValue v) (.getIntegerValue v)
    (.hasDoubleValue v) (.getDoubleValue v)
    (.hasBoolValue v)   (.getBoolValue v)
    (.hasListValue v)   (mapv value->clj (.. v getListValue getValuesList))
    (.hasStructValue v) (reduce-kv (fn [m k vv] (assoc m (keyword k) (value->clj vv)))
                                   {}
                                   (into {} (.. v getStructValue getFieldsMap)))
    :else nil))

(defn point->map
  "Convert a qdrant RetrievedPoint or ScoredPoint into a {:id :payload :score?} map."
  [point]
  (let [payload (try (.getPayloadMap point) (catch Throwable _ {}))
        decoded (reduce-kv (fn [m k v] (assoc m (keyword k) (value->clj v)))
                           {}
                           (into {} payload))
        id      (try (.. point getId getUuid) (catch Throwable _ nil))]
    (cond-> {:payload decoded}
      id (assoc :id id))))

(defn scroll-points
  "Paginated scroll over a collection.

   kw-args:
     :collection — collection name (required)
     :limit      — page size (default 100)
     :filter     — optional Common$Filter built via ->filter

   Returns {:collection :count :points} where :points is a vec of point->map."
  [{:keys [client]} & {:keys [collection limit filter]
                       :or   {limit 100}}]
  (let [b (-> (Points$ScrollPoints/newBuilder)
              (.setCollectionName ^String collection)
              (.setLimit (int limit))
              (.setWithPayload (-> (Points$WithPayloadSelector/newBuilder)
                                   (.setEnable true)
                                   .build)))]
    (when filter (.setFilter b filter))
    (let [resp   (.get (.scrollAsync client (.build b)))
          points (try (.getResultList resp) (catch Throwable _ []))]
      {:collection collection
       :count      (count points)
       :points     (mapv point->map points)})))
