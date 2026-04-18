# clj-qdrant

Idiomatic Clojure facade over the official
[qdrant/java-client](https://github.com/qdrant/java-client).

The java client owns all transport concerns (gRPC channels, TLS, auth,
keepalive). This library is a pure data-shape adapter: Clojure maps in,
Clojure maps out.

## Usage

```clojure
(require '[clj-qdrant.client :as c]
         '[clj-qdrant.schema :as s]
         '[clj-qdrant.api    :as api]
         '[clj-qdrant.index  :as idx])

(def cli (c/make {:host "localhost" :port 6334}))

(s/collection-create! cli
  {:name "memories" :vector-size 384 :distance :cosine
   :hnsw {:m 16 :ef-construct 256}})

(idx/keyword-index! cli {:collection "memories" :field-name "tag"})

(api/upsert-points cli
  :collection "memories"
  :points [{:id 1 :vector (repeat 384 0.1) :payload {:tag "hi"}}])

(api/search-points cli
  :collection "memories"
  :vector (repeat 384 0.1)
  :limit 10)

(c/close! cli)
```

## Namespaces

| ns                    | purpose                                    |
|-----------------------|--------------------------------------------|
| `clj-qdrant.config`   | env-driven default config (atom)           |
| `clj-qdrant.client`   | `make` / `close!` / `with-client` facade   |
| `clj-qdrant.schema`   | collection + vector + HNSW + distance      |
| `clj-qdrant.index`    | payload index helpers                      |
| `clj-qdrant.api`      | upsert / search / delete / get / scroll    |

## Test

```bash
clj -M:test
```

Integration tests gated behind `^:integration` metadata. Run the full
suite against a live qdrant with:

```bash
clj -M:test -i :integration
```

## References

- Qdrant Java Client: <https://github.com/qdrant/java-client>
- Qdrant gRPC API:   <https://api.qdrant.tech/>
