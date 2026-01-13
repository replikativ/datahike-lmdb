(ns datahike-lmdb.benchmark
  "Benchmark comparing datahike-lmdb vs Datalevin"
  (:require [datahike.api :as d]
            [datahike-lmdb.core]
            [datalevin.core :as dl]))

(def base-path "tmp/db")

(def test-sizes [100000 500000 1000000])

(def schema
  [{:db/ident :person/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :person/age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :person/email
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

(def datalevin-schema
  {:person/name  {:db/valueType :db.type/string}
   :person/age   {:db/valueType :db.type/long}
   :person/email {:db/valueType :db.type/string}})

(defn generate-entities [n]
  (mapv (fn [i]
          {:person/name (str "Person-" i)
           :person/age (+ 20 (mod i 50))
           :person/email (str "person" i "@example.com")})
        (range n)))

(defn cleanup-dir [path]
  (let [f (java.io.File. path)]
    (when (.exists f)
      (run! #(.delete %) (reverse (file-seq f))))))

;;; Datahike-LMDB benchmarks

(defn bench-datahike-lmdb-write [n]
  (let [path (str base-path "/datahike-lmdb")
        _ (cleanup-dir path)
        cfg {:store {:backend :lmdb
                     :path path
                     :id (java.util.UUID/randomUUID)}
             :attribute-refs? true
             :schema-flexibility :write
             :keep-history? false}
        _ (d/create-database cfg)
        conn (d/connect cfg)
        entities (generate-entities n)
        _ (d/transact conn schema)
        start (System/nanoTime)
        _ (d/transact conn entities)
        elapsed (/ (- (System/nanoTime) start) 1e9)]
    (d/release conn)
    (cleanup-dir path)
    {:backend :datahike-lmdb
     :operation :write
     :entities n
     :time-sec elapsed
     :entities-per-sec (/ n elapsed)}))

(defn bench-datahike-lmdb-query [n]
  (let [path (str base-path "/datahike-lmdb")
        _ (cleanup-dir path)
        cfg {:store {:backend :lmdb
                     :path path
                     :id (java.util.UUID/randomUUID)}
             :schema-flexibility :write
             :keep-history? false}
        _ (d/create-database cfg)
        conn (d/connect cfg)
        entities (generate-entities n)
        _ (d/transact conn schema)
        _ (d/transact conn entities)
        db @conn
        start (System/nanoTime)
        result (d/q '[:find ?n ?a
                      :where
                      [?e :person/name ?n]
                      [?e :person/age ?a]
                      [(> ?a 40)]]
                    db)
        elapsed (/ (- (System/nanoTime) start) 1e9)]
    (d/release conn)
    (cleanup-dir path)
    {:backend :datahike-lmdb
     :operation :query
     :entities n
     :results (count result)
     :time-sec elapsed}))

;;; Datalevin benchmarks

(defn bench-datalevin-write [n]
  (let [path (str base-path "/datalevin")
        _ (cleanup-dir path)
        conn (dl/get-conn path datalevin-schema)
        entities (generate-entities n)
        start (System/nanoTime)
        _ (dl/transact! conn entities)
        elapsed (/ (- (System/nanoTime) start) 1e9)]
    (dl/close conn)
    (cleanup-dir path)
    {:backend :datalevin
     :operation :write
     :entities n
     :time-sec elapsed
     :entities-per-sec (/ n elapsed)}))

(defn bench-datalevin-query [n]
  (let [path (str base-path "/datalevin")
        _ (cleanup-dir path)
        conn (dl/get-conn path datalevin-schema)
        entities (generate-entities n)
        _ (dl/transact! conn entities)
        db (dl/db conn)
        start (System/nanoTime)
        result (dl/q '[:find ?n ?a
                       :where
                       [?e :person/name ?n]
                       [?e :person/age ?a]
                       [(> ?a 40)]]
                     db)
        elapsed (/ (- (System/nanoTime) start) 1e9)]
    (dl/close conn)
    (cleanup-dir path)
    {:backend :datalevin
     :operation :query
     :entities n
     :results (count result)
     :time-sec elapsed}))

;;; Run all benchmarks

(defn run-benchmarks []
  (println "\n=== Datahike-LMDB vs Datalevin Benchmark ===")
  (println (str "Path: " base-path))
  (println)

  (doseq [n test-sizes]
    (println (str "\n--- " n " entities ---"))

    ;; Write benchmarks
    (print "Datalevin write... ") (flush)
    (let [r (bench-datalevin-write n)]
      (println (format "%.3f sec (%.0f entities/sec)" (:time-sec r) (:entities-per-sec r))))

    (print "Datahike-LMDB write... ") (flush)
    (let [r (bench-datahike-lmdb-write n)]
      (println (format "%.3f sec (%.0f entities/sec)" (:time-sec r) (:entities-per-sec r))))

    ;; Query benchmarks
    (print "Datalevin query... ") (flush)
    (let [r (bench-datalevin-query n)]
      (println (format "%.3f sec (%d results)" (:time-sec r) (:results r))))

    (print "Datahike-LMDB query... ") (flush)
    (let [r (bench-datahike-lmdb-query n)]
      (println (format "%.3f sec (%d results)" (:time-sec r) (:results r)))))

  (println "\n=== Done ==="))

(defn -main [& _args]
  (run-benchmarks))
