# datahike-lmdb

LMDB storage backend for [Datahike](https://github.com/replikativ/datahike).

## Features

- High-performance LMDB storage via [konserve-lmdb](https://github.com/replikativ/konserve-lmdb)
- Native buffer encoding for PSS types (zero-copy reads)
- Batched writes for improved throughput

## Requirements

- Java 22+ (Project Panama FFI)
- liblmdb native library
- Set `KONSERVE_LMDB_LIB` environment variable to path of liblmdb.so if it is not on a standard one

## Usage

Add to deps.edn:

```clojure
{:deps {io.replikativ/datahike-lmdb {:git/url "https://github.com/replikativ/datahike-lmdb" :git/sha "LATEST"}}}
```

Use in code:

```clojure
(require '[datahike.api :as d])
(require '[datahike-lmdb.core])  ;; Registers :lmdb backend

(def cfg {:store {:backend :lmdb
                  :path "/path/to/database"}
          :schema-flexibility :write
          :keep-history? false})

(d/create-database cfg)
(def conn (d/connect cfg))

;; Use datahike normally
(d/transact conn [{:db/ident :person/name
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one}])

(d/transact conn [{:person/name "Alice"}])

(d/q '[:find ?n :where [?e :person/name ?n]] @conn)
;; => #{["Alice"]}

(d/release conn)
```

## Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `:path` | Directory path for LMDB data files | (required) |
| `:map-size` | LMDB map size in bytes | 1GB |
| `:cache-size` | Node cache size | 1000 |
| `:scope` | Scope identifier | hostname |

## Benchmarks

Three benchmark suites are available in the `bench/` directory to evaluate performance:

### 1. Basic Benchmark (`benchmark.clj`)
Compares datahike-lmdb vs Datalevin for simple person entities with write and query operations.

```bash
clojure -M:bench/basic
# Or with the base alias:
clojure -M:bench -m datahike-lmdb.benchmark
```

Tests 100k, 500k, and 1M entities with small records.

### 2. Wikipedia Benchmark (`wiki_benchmark.clj`)
Full Wikipedia article import simulation comparing datahike-lmdb vs Datalevin. Includes query benchmarks for common workloads:
- Point lookups by `:article/page-id` (unique identity)
- Equality filter on `:article/category` (AVET index)
- Range filter on `:article/byte-size` (numeric index)

```bash
# Run with default 50,000 articles in batches of 1,000
clojure -M:bench/wiki

# Run with custom article count and batch size
clojure -M:bench -m datahike-lmdb.wiki-benchmark 5000 500
```

Simulates realistic article size distribution (stubs, typical, detailed, featured) with 16 attributes per article. Query metrics are printed for both backends (total seconds and QPS for each query type).

**Note:** Benchmark data is written to `tmp/db/` and cleaned up automatically after each run.

## License

Copyright © 2026 Christian Weilbach

Distributed under the Eclipse Public License version 2.0.
