(ns clj-qdrant.schema
  "Qdrant collection schema helpers: vector params, HNSW config,
   distance metric, payload schema types, and create/recreate entrypoints.

   Mirrors milvus-clj.schema in intent — Clojure maps in, Java builder
   objects out — adapted to qdrant's grpc.Collections namespace."
  (:import [io.qdrant.client.grpc Collections$Distance
                                  Collections$VectorParams
                                  Collections$VectorsConfig
                                  Collections$CreateCollection
                                  Collections$HnswConfigDiff
                                  Collections$PayloadSchemaType]))

;; ---------------------------------------------------------------------------
;; Distance metric
;; ---------------------------------------------------------------------------

(def distances
  "Keyword → qdrant Distance enum."
  {:cosine    Collections$Distance/Cosine
   :euclid    Collections$Distance/Euclid
   :dot       Collections$Distance/Dot
   :manhattan Collections$Distance/Manhattan})

(defn ->distance
  "Resolve keyword or Distance instance to a Distance enum value."
  [d]
  (cond
    (instance? Collections$Distance d) d
    (keyword? d) (or (get distances d)
                     (throw (ex-info (str "Unknown distance: " d)
                                     {:distance d
                                      :valid    (keys distances)})))
    :else (throw (ex-info "distance must be keyword or Distance"
                          {:got (type d)}))))

;; ---------------------------------------------------------------------------
;; Vector + HNSW params
;; ---------------------------------------------------------------------------

(defn vectors-config
  "Build a VectorsConfig for a single unnamed vector.

   opts:
     :size     — vector dimension (required)
     :distance — distance metric keyword (:cosine/:euclid/:dot/:manhattan)"
  [{:keys [size distance]}]
  (let [params (-> (Collections$VectorParams/newBuilder)
                   (.setSize (long size))
                   (.setDistance (->distance distance))
                   .build)]
    (-> (Collections$VectorsConfig/newBuilder)
        (.setParams params)
        .build)))

(defn hnsw-config
  "Build an HnswConfigDiff from a map.

   opts:
     :m                    — connections per node
     :ef-construct         — build-time search width
     :full-scan-threshold  — threshold for full-scan fallback"
  [{:keys [m ef-construct full-scan-threshold]}]
  (let [b (Collections$HnswConfigDiff/newBuilder)]
    (when m                   (.setM b (int m)))
    (when ef-construct        (.setEfConstruct b (int ef-construct)))
    (when full-scan-threshold (.setFullScanThreshold b (int full-scan-threshold)))
    (.build b)))

;; ---------------------------------------------------------------------------
;; Collection lifecycle
;; ---------------------------------------------------------------------------

(defn collection-create!
  "Create a qdrant collection.

   opts:
     :name        — collection name
     :vector-size — embedding dimension
     :distance    — :cosine / :euclid / :dot / :manhattan
     :hnsw        — optional HNSW config map"
  [{:keys [client]} {:keys [name vector-size distance hnsw]}]
  (let [vc (vectors-config {:size vector-size :distance distance})
        b  (-> (Collections$CreateCollection/newBuilder)
               (.setCollectionName ^String name)
               (.setVectorsConfig vc))]
    (when hnsw (.setHnswConfig b (hnsw-config hnsw)))
    (.get (.createCollectionAsync client (.build b)))
    {:name        name
     :vector-size vector-size
     :distance    distance}))

(defn collection-recreate!
  "Drop the collection if it exists, then create fresh."
  [{:keys [client] :as cm} {:keys [name] :as opts}]
  (try (.get (.deleteCollectionAsync client ^String name))
       (catch Throwable _ nil))
  (collection-create! cm opts))

;; ---------------------------------------------------------------------------
;; Payload schema types
;; ---------------------------------------------------------------------------

(def payload-schema-types
  "Keyword → qdrant PayloadSchemaType enum."
  {:keyword Collections$PayloadSchemaType/Keyword
   :integer Collections$PayloadSchemaType/Integer
   :float   Collections$PayloadSchemaType/Float
   :geo     Collections$PayloadSchemaType/Geo
   :text    Collections$PayloadSchemaType/Text
   :bool    Collections$PayloadSchemaType/Bool})

(defn payload-index-create!
  "Create a payload index on a single field.

   opts:
     :collection — collection name
     :field      — field name in the payload
     :type       — schema keyword (:keyword/:integer/:float/:geo/:text/:bool)"
  [{:keys [client]} {:keys [collection field type]}]
  (let [schema (or (get payload-schema-types type)
                   (throw (ex-info (str "Unknown payload-schema type: " type)
                                   {:type type :valid (keys payload-schema-types)})))]
    (.get (.createPayloadIndexAsync client
                                    ^String collection
                                    ^String field
                                    schema
                                    nil
                                    nil
                                    nil
                                    nil))
    {:collection collection :field field :type type}))
