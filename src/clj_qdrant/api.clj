(ns clj-qdrant.api
  "Point CRUD + search over the official qdrant java client.

   All functions take a client-map (from `clj-qdrant.client/make`) and
   kw-args. Return values are Clojure maps summarizing the operation —
   raw java responses are stashed under :results / :points when useful."
  (:import [io.qdrant.client ConditionFactory PointIdFactory ValueFactory VectorsFactory]
           [io.qdrant.client.grpc Common$Filter
                                  JsonWithInt$Value
                                  Points$PayloadIncludeSelector
                                  Points$PointStruct
                                  Points$ReadConsistency
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

(declare ->with-payload-selector scored-point->map)

(defn search-points
  "Vector similarity search.

   kw-args:
     :collection       — collection name (required)
     :vector           — query vector (required)
     :limit            — max points (default 10)
     :filter           — optional Common$Filter (build via `->filter`)
     :with-payload?    — request the point payload (default true)
     :payload-includes — optional field-name projection (implies payload)

   qdrant's gRPC default is with_payload=false: without an explicit
   WithPayloadSelector a ScoredPoint carries only id+score, so any
   downstream decode loses :tags/:content. Payload is requested by default.

   Returns {:collection :count :results} where :results is a vec of
   `scored-point->map` — {:id :payload :score}. Raw protobuf never escapes
   this boundary (mirrors `scroll-points`)."
  [{:keys [client]} & {:keys [collection vector limit filter with-payload? payload-includes]
                       :or   {limit 10 with-payload? true}}]
  (let [b (-> (Points$SearchPoints/newBuilder)
              (.setCollectionName ^String collection)
              (.addAllVector (map float vector))
              (.setLimit (long limit)))]
    (when (or with-payload? (seq payload-includes))
      (.setWithPayload b (->with-payload-selector payload-includes)))
    (when filter (.setFilter b filter))
    (let [res (.get (.searchAsync client (.build b)))]
      {:collection collection
       :count      (count res)
       :results    (mapv scored-point->map res)})))

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
  (let [^Points$ReadConsistency consistency nil
        res (.get (.retrieveAsync client
                                  ^String collection
                                  (mapv ->point-id ids)
                                  true true
                                  consistency))]
    {:collection collection
     :points     (vec res)}))

(defn ->filter
  "Build a Common$Filter from a map of must-conditions.

   opts:
     :must-keyword     {field-name [string ...]}  — AND of single-keyword
                                                    matches per value (one
                                                    qdrant Condition per
                                                    keyword); appropriate for
                                                    list-of-tags fields where
                                                    the caller wants every
                                                    listed tag present.
     :must-match-any   {field-name [string ...]}  — OR over the list via
                                                    qdrant `matchKeywords`
                                                    (MatchAny). Use for
                                                    single-valued fields like
                                                    :project-id where the
                                                    caller wants entries
                                                    matching ANY of the
                                                    listed values.
     :must-keywords-all [{:field f :value v}]     — AND of single-keyword
                                                    matches (explicit form).

   Returns a Common$Filter or nil if no conditions provided."
  [{:keys [must-keyword must-match-any must-keywords-all]}]
  (let [b (Common$Filter/newBuilder)
        any-added? (atom false)]
    (doseq [[field values] must-keyword
            :when (seq values)]
      ;; Per-value matchKeyword loop = AND-of-keywords. Each Condition is a
      ;; separate MUST clause, so an entry must match every value.
      (doseq [v values]
        (.addMust b (ConditionFactory/matchKeyword (name field) ^String v))
        (reset! any-added? true)))
    (doseq [[field values] must-match-any
            :when (seq values)]
      ;; matchKeywords (plural) = MatchAny = OR-of-keywords on a single
      ;; field. One MUST clause whose match expression accepts any of the
      ;; supplied values. Required for HCR multi-scope `:project-id IN [...]`.
      (.addMust b (ConditionFactory/matchKeywords (name field)
                                                  ^java.util.List (mapv str values)))
      (reset! any-added? true))
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
        id      (try (.. point getId getUuid) (catch Throwable _ nil))
        ;; ScoredPoint carries a similarity score; RetrievedPoint does not
        ;; (the method is absent → caught → nil, so no :score key).
        score   (try (.getScore point) (catch Throwable _ nil))]
    (cond-> {:payload decoded}
      id    (assoc :id id)
      score (assoc :score score))))

(defn scored-point->map
  "Convert a qdrant ScoredPoint into {:id :payload :score}.

   `point->map` deliberately drops the score (RetrievedPoint has none); a
   similarity search whose rows lost their score is unrankable, so the
   search boundary decodes through this fn instead."
  [point]
  (assoc (point->map point)
         :score (try (.getScore point) (catch Throwable _ nil))))

(def ^:private scroll-page-size
  "Per-page internal limit for paginated scroll. Keeps individual gRPC
   responses small; the user-visible :limit is honored by the accumulator."
  32)

(defn- ->with-payload-selector
  "Build a Points$WithPayloadSelector.

   `payload-includes` — seq of field names (string/keyword). When seq is
   non-empty, wire a PayloadIncludeSelector so qdrant returns only those
   payload keys. When nil/empty, fall back to full-payload (:enable true)."
  [payload-includes]
  (let [b (Points$WithPayloadSelector/newBuilder)]
    (if (seq payload-includes)
      (let [fields (mapv name payload-includes)
            inc    (-> (Points$PayloadIncludeSelector/newBuilder)
                       (.addAllFields fields)
                       .build)]
        (.setInclude b inc))
      (.setEnable b true))
    (.build b)))

(defn- scroll-page
  "One gRPC scroll round-trip. Returns {:points [raw-points] :next-offset id-or-nil}.

   `payload-includes` (optional) — vec of string/keyword field names. When
   provided, qdrant responds with only those payload keys (projection)."
  [client collection filter page-limit offset payload-includes]
  (let [b (-> (Points$ScrollPoints/newBuilder)
              (.setCollectionName ^String collection)
              (.setLimit (int page-limit))
              (.setWithPayload (->with-payload-selector payload-includes)))]
    (when filter (.setFilter b filter))
    (when offset (.setOffset b offset))
    (let [resp     (.get (.scrollAsync client (.build b)))
          points   (try (.getResultList resp) (catch Throwable _ []))
          next-off (try
                     (when (.hasNextPageOffset resp)
                       (.getNextPageOffset resp))
                     (catch Throwable _ nil))]
      {:points points :next-offset next-off})))

(defn- paginate-scroll
  "Loop `scroll-fn` until `limit` points accumulated or no next-offset.
   `scroll-fn`: (fn [offset page-limit]) -> {:points coll :next-offset id-or-nil}.
   Returns a vec of raw (undecoded) points."
  [scroll-fn limit page-size]
  (loop [offset nil
         acc    []]
    (let [remaining (- limit (count acc))]
      (if (<= remaining 0)
        acc
        (let [page-limit            (min page-size remaining)
              {:keys [points
                      next-offset]} (scroll-fn offset page-limit)
              acc'                  (into acc points)]
          (if (and next-offset (seq points))
            (recur next-offset acc')
            acc'))))))

(defn scroll-points
  "Paginated scroll over a collection.

   Loops internally on ScrollResponse.getNextPageOffset, paging at
   `scroll-page-size` per gRPC round-trip. Accumulates until the user's
   :limit is reached or there is no next-page-offset.

   kw-args:
     :collection       — collection name (required)
     :limit            — total points to return (default 100)
     :filter           — optional Common$Filter built via ->filter
     :payload-includes — optional vec of field names (string/keyword).
                         When non-empty, qdrant returns ONLY those payload
                         keys (server-side projection). Defaults to full
                         payload when nil/empty.

   Returns {:collection :count :points} where :points is a vec of point->map."
  [{:keys [client]} & {:keys [collection limit filter payload-includes]
                       :or   {limit 100}}]
  (let [scroll-fn (fn [offset page-limit]
                    (scroll-page client collection filter
                                 page-limit offset payload-includes))
        raw       (paginate-scroll scroll-fn limit scroll-page-size)]
    {:collection collection
     :count      (count raw)
     :points     (mapv point->map raw)}))
