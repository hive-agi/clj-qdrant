(ns clj-qdrant.index
  "Qdrant payload index helpers.

   Wraps QdrantClient.createPayloadIndexAsync with idiomatic keyword
   dispatch for field types (keyword/integer/float/geo/text/bool)."
  (:require [clj-qdrant.schema :as schema]))

(defn create-payload-index!
  "Create a payload index on a field.

   opts:
     :collection — collection name
     :field-name — payload key to index
     :field-type — :keyword / :integer / :float / :geo / :text / :bool"
  [{:keys [client]} {:keys [collection field-name field-type]}]
  (let [pst (or (get schema/payload-schema-types field-type)
                (throw (ex-info (str "Unknown payload schema type: " field-type)
                                {:field-type field-type
                                 :valid      (keys schema/payload-schema-types)})))]
    (.get (.createPayloadIndexAsync client
                                    ^String collection
                                    ^String field-name
                                    pst
                                    nil nil nil nil))
    {:collection collection
     :field      field-name
     :type       field-type}))

(defn keyword-index!
  [cm opts]
  (create-payload-index! cm (assoc opts :field-type :keyword)))

(defn integer-index!
  [cm opts]
  (create-payload-index! cm (assoc opts :field-type :integer)))

(defn float-index!
  [cm opts]
  (create-payload-index! cm (assoc opts :field-type :float)))

(defn geo-index!
  [cm opts]
  (create-payload-index! cm (assoc opts :field-type :geo)))

(defn text-index!
  [cm opts]
  (create-payload-index! cm (assoc opts :field-type :text)))

(defn bool-index!
  [cm opts]
  (create-payload-index! cm (assoc opts :field-type :bool)))
