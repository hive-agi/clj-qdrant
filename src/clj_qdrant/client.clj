(ns clj-qdrant.client
  "Thin Clojure facade over io.qdrant.client.QdrantClient + QdrantGrpcClient.

   The java client owns all transport concerns (channels, TLS, keepalive,
   auth). This namespace is a pure data-shape adapter: Clojure map in,
   Clojure map out. No protocols, no defrecords, no socket plumbing."
  (:import [io.qdrant.client QdrantClient QdrantGrpcClient]
           [io.grpc ManagedChannelBuilder]))

(def ^:const default-max-inbound-message-size
  "Default gRPC max inbound message size: 64 MiB.
   Qdrant responses for large collections / scroll batches can exceed the
   gRPC default (4 MiB) and fail with RESOURCE_EXHAUSTED."
  (* 64 1024 1024))

(defn make
  "Construct a qdrant client wrapper.

   Opts:
     :host                       — qdrant host (default \"localhost\")
     :port                       — gRPC port (default 6334)
     :api-key                    — optional API key for Qdrant Cloud
     :tls?                       — enable TLS (default false)
     :max-inbound-message-size   — gRPC max inbound bytes
                                   (default 64 MiB; raise to avoid
                                   RESOURCE_EXHAUSTED on large scroll/search
                                   responses)

   Returns a map `{:client QdrantClient :grpc-client QdrantGrpcClient :config {..}}`.
   Pass this map to functions in `clj-qdrant.api` / `clj-qdrant.schema`."
  [{:keys [host port api-key tls? max-inbound-message-size]
    :or   {host "localhost"
           port 6334
           tls? false
           max-inbound-message-size default-max-inbound-message-size}}]
  (let [chan-b  (-> (ManagedChannelBuilder/forAddress ^String host (int port))
                    (.maxInboundMessageSize (int max-inbound-message-size)))
        chan-b  (if tls?
                  (.useTransportSecurity chan-b)
                  (.usePlaintext chan-b))
        channel (.build chan-b)
        b       (QdrantGrpcClient/newBuilder channel)
        b       (if api-key (.withApiKey b ^String api-key) b)
        grpc    (.build b)
        client  (QdrantClient. grpc)]
    {:client      client
     :grpc-client grpc
     :config      {:host                     host
                   :port                     port
                   :tls?                     tls?
                   :api-key                  (when api-key "***")
                   :max-inbound-message-size max-inbound-message-size}}))

(defn close!
  "Close the underlying QdrantClient. Idempotent."
  [{:keys [client]}]
  (try (when client (.close client)) (catch Throwable _ nil)))

(defmacro with-client
  "Open a client, run body, close deterministically."
  [[binding opts] & body]
  `(let [~binding (make ~opts)]
     (try ~@body (finally (close! ~binding)))))
