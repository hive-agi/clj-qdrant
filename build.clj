(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.string :as str]))

(def lib 'io.github.hive-agi/clj-qdrant)
(def version (str/trim (slurp "VERSION")))
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))
(def basis (delay (b/create-basis {:project "deps.edn"})))

(def readme "README.md")

(defn coord-patterns
  "Regexes matching the README install coordinates as (prefix)(version)(suffix).
   Derived from `lib`, so a group/artifact rename can't silently orphan them."
  []
  (let [l (java.util.regex.Pattern/quote (str lib))]
    [;; deps.edn:   io.github.hive-agi/clj-qdrant {:mvn/version "X"}
     (re-pattern (str "(" l " \\{:mvn/version \")([^\"]+)(\")"))
     ;; Leiningen:  [io.github.hive-agi/clj-qdrant "X"]
     (re-pattern (str "(\\[" l " \")([^\"]+)(\")"))]))

(defn readme-versions
  "Every version currently pinned in the README's install coordinates."
  [source]
  (into [] (mapcat #(map second (re-seq % source))) (coord-patterns)))

(defn- sync-readme
  "Rewrite the README install coordinates to `version`. Returns the new source."
  [source]
  (reduce (fn [s re] (str/replace s re (str "$1" version "$3")))
          source
          (coord-patterns)))

(def pom-data
  [[:description "Idiomatic Clojure facade over the official Qdrant java-client: a pure data-shape adapter, Clojure maps in, Clojure maps out."]
   [:url "https://github.com/hive-agi/clj-qdrant"]
   [:licenses
    [:license
     [:name "MIT"]
     [:url "https://opensource.org/license/mit"]]]
   [:scm
    [:url "https://github.com/hive-agi/clj-qdrant"]
    [:connection "scm:git:git://github.com/hive-agi/clj-qdrant.git"]
    [:developerConnection "scm:git:ssh://git@github.com/hive-agi/clj-qdrant.git"]
    [:tag (str "v" version)]]
   [:developers
    [:developer
     [:name "Pedro G. Branquinho"]]]])

(defn clean [_]
  (b/delete {:path "target"}))

(defn sync-version
  "Propagate the canonical top-level VERSION to everything that restates it:
   currently just the README install coordinates."
  [_]
  (let [before (slurp readme)
        after  (sync-readme before)]
    (when (not= before after)
      (spit readme after))
    (println (str "Synced " readme " install coords -> " version
                  (when (= before after) " (already current)")))))

(defn jar
  "Build the library thin jar + pom for Clojars/Maven consumption."
  [_]
  (clean nil)
  (sync-version nil)
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis @basis
                :src-dirs ["src"]
                :pom-data pom-data})
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file})
  (println (str "Built " jar-file)))

(defn deploy
  "Deploy the library jar to Clojars. Requires CLOJARS_USERNAME + CLOJARS_PASSWORD
   (a Clojars deploy token) in the environment."
  [_]
  (jar nil)
  ((requiring-resolve 'deps-deploy.deps-deploy/deploy)
   {:installer :remote
    :artifact  jar-file
    :pom-file  (b/pom-path {:lib lib :class-dir class-dir})})
  (println (str "Deployed " lib " " version " to Clojars")))
