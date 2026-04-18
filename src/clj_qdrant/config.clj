(ns clj-qdrant.config
  "Qdrant connection configuration and defaults.

   Manages host, port, api-key, and TLS settings for the gRPC
   transport owned by the official qdrant/java-client.

   Mirrors the shape of milvus-clj.config — atom-held config with
   configure!/reset-config! lifecycle, env-var driven defaults.")

;; ---------------------------------------------------------------------------
;; Defaults
;; ---------------------------------------------------------------------------

(def ^:private default-config
  {:host               (or (System/getenv "QDRANT_HOST") "localhost")
   ;; Qdrant gRPC default is 6334 (6333 is the REST port).
   :port               (parse-long (or (System/getenv "QDRANT_PORT") "6334"))
   :api-key            (System/getenv "QDRANT_API_KEY")
   :tls?               (= "true" (System/getenv "QDRANT_TLS"))
   :connect-timeout-ms 10000})

;; ---------------------------------------------------------------------------
;; State
;; ---------------------------------------------------------------------------

(defonce ^:private config (atom default-config))

;; ---------------------------------------------------------------------------
;; Public API
;; ---------------------------------------------------------------------------

(defn get-config
  "Current Qdrant configuration map."
  []
  @config)

(defn configure!
  "Merge new settings into the Qdrant configuration.

   Example:
     (configure! {:host \"qdrant.prod\" :port 6334 :api-key \"secret\"})"
  [opts]
  (swap! config merge opts)
  @config)

(defn reset-config!
  "Reset configuration to defaults."
  []
  (reset! config default-config))
