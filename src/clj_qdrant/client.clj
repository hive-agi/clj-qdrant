(ns clj-qdrant.client
  "Thin Clojure facade over io.qdrant.client.QdrantClient + QdrantGrpcClient.

   The java client owns all transport concerns (channels, TLS, keepalive,
   auth). This namespace is a pure data-shape adapter: Clojure map in,
   Clojure map out. No protocols, no defrecords, no socket plumbing."
  (:import [io.qdrant.client QdrantClient QdrantGrpcClient]))

(defn make
  "Construct a qdrant client wrapper.

   Opts:
     :host     — qdrant host (default \"localhost\")
     :port     — gRPC port (default 6334)
     :api-key  — optional API key for Qdrant Cloud
     :tls?     — enable TLS (default false)

   Returns a map `{:client QdrantClient :grpc-client QdrantGrpcClient :config {..}}`.
   Pass this map to functions in `clj-qdrant.api` / `clj-qdrant.schema`."
  [{:keys [host port api-key tls?]
    :or   {host "localhost" port 6334 tls? false}}]
  (let [b      (QdrantGrpcClient/newBuilder ^String host (int port) (boolean tls?))
        b      (if api-key (.withApiKey b ^String api-key) b)
        grpc   (.build b)
        client (QdrantClient. grpc)]
    {:client      client
     :grpc-client grpc
     :config      {:host host
                   :port port
                   :tls? tls?
                   :api-key (when api-key "***")}}))

(defn close!
  "Close the underlying QdrantClient. Idempotent."
  [{:keys [client]}]
  (try (when client (.close client)) (catch Throwable _ nil)))

(defmacro with-client
  "Open a client, run body, close deterministically."
  [[binding opts] & body]
  `(let [~binding (make ~opts)]
     (try ~@body (finally (close! ~binding)))))
