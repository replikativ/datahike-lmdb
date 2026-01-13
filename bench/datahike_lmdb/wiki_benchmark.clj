(ns datahike-lmdb.wiki-benchmark
  "Benchmark simulating Wikipedia article import with realistic wikitext sizes"
  (:require [datahike.api :as d]
            [datahike-lmdb.core]
            [datalevin.core :as dl])
  (:import [java.util UUID]))

(def base-path "tmp/db")

;; Wikipedia article sizes (from actual distribution):
;; - Median: ~5KB
;; - Mean: ~15KB
;; - 90th percentile: ~30KB
;; - Some articles: 100KB+

(def article-sizes
  "Realistic distribution of article sizes in chars"
  {:small 2000      ; stub articles
   :medium 8000     ; typical articles
   :large 25000     ; detailed articles
   :huge 80000})    ; featured articles

(def size-distribution
  "Weighted distribution matching Wikipedia"
  (vec (concat
        (repeat 30 :small)    ; 30% stubs
        (repeat 45 :medium)   ; 45% typical
        (repeat 20 :large)    ; 20% detailed
        (repeat 5 :huge))))   ; 5% featured

;; Datahike schema (matching einbetten)
(def datahike-schema
  [{:db/ident :article/page-id
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :article/title
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :article/namespace
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :article/redirect
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :article/category
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many
    :db/index true}
   {:db/ident :article/revision-id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :article/parent-revision-id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :article/timestamp
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :article/contributor-name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :article/contributor-id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :article/contributor-ip
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :article/edit-comment
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :article/minor-edit
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one}
   {:db/ident :article/byte-size
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :article/sha1
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :article/wikitext
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

;; Datalevin schema
(def datalevin-schema
  {:article/page-id {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :article/title {:db/valueType :db.type/string}
   :article/namespace {:db/valueType :db.type/long}
   :article/redirect {:db/valueType :db.type/string}
   :article/category {:db/valueType :db.type/string :db/cardinality :db.cardinality/many}
   :article/revision-id {:db/valueType :db.type/string}
   :article/parent-revision-id {:db/valueType :db.type/string}
   :article/timestamp {:db/valueType :db.type/instant}
   :article/contributor-name {:db/valueType :db.type/string}
   :article/contributor-id {:db/valueType :db.type/string}
   :article/contributor-ip {:db/valueType :db.type/string}
   :article/edit-comment {:db/valueType :db.type/string}
   :article/minor-edit {:db/valueType :db.type/boolean}
   :article/byte-size {:db/valueType :db.type/long}
   :article/sha1 {:db/valueType :db.type/string}
   :article/wikitext {:db/valueType :db.type/string}})

(defn generate-wikitext
  "Generate fake wikitext of given size"
  [size]
  (let [paragraphs ["Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                    "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
                    "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris. "
                    "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum. "
                    "== Section Header ==\n\n"
                    "{{Infobox | title = Example | data = value }}\n"
                    "[[Category:Example Category]]\n"
                    "[[Link to another article]]\n"
                    "'''Bold text''' and ''italic text'' are common.\n"
                    "<ref>Citation needed</ref>\n"]
        sb (StringBuilder.)]
    (while (< (.length sb) size)
      (.append sb (rand-nth paragraphs)))
    (.substring (.toString sb) 0 (min size (.length sb)))))

(defn generate-article
  "Generate a realistic Wikipedia article entity"
  [i]
  (let [size-key (rand-nth size-distribution)
        text-size (get article-sizes size-key)
        num-categories (+ 1 (rand-int 8))]
    {:article/page-id (str i)
     :article/title (str "Article_" i "_" (UUID/randomUUID))
     :article/namespace 0
     :article/category (mapv #(str "Category_" %) (range num-categories))
     :article/revision-id (str (+ 1000000000 i))
     :article/parent-revision-id (str (+ 999999999 i))
     :article/timestamp (java.util.Date.)
     :article/contributor-name (str "User_" (rand-int 100000))
     :article/contributor-id (str (rand-int 100000))
     :article/edit-comment "Updated content"
     :article/minor-edit (< (rand) 0.3)
     :article/byte-size text-size
     :article/sha1 (str (UUID/randomUUID))
     :article/wikitext (generate-wikitext text-size)}))

(defn cleanup-dir [path]
  (let [f (java.io.File. path)]
    (when (.exists f)
      (run! #(.delete %) (reverse (file-seq f))))))

(defn now-ns []
  (System/nanoTime))

(defn elapsed-sec [start-ns end-ns]
  (/ (- end-ns start-ns) 1e9))

;;; Benchmark with batches (simulating streaming import)

(defn bench-datahike-lmdb-wiki
  "Benchmark datahike-lmdb with Wikipedia-like data in batches"
  [total-articles batch-size]
  (let [t0 (now-ns)
        path (str base-path "/datahike-lmdb-wiki")
        _ (cleanup-dir path)
        cfg {:store {:backend :lmdb
                     :path path
                     :id (UUID/randomUUID)
                     :map-size (* 10 1024 1024 1024)} ;; 10GB map
             :schema-flexibility :write
             :attribute-refs? true
             :keep-history? false}
        _ (d/create-database cfg)
        conn (d/connect cfg)
        _ (d/transact conn datahike-schema)
        t-setup-done (now-ns)
        num-batches (/ total-articles batch-size)
        ;; Print roughly 10 progress lines regardless of total batches
        print-every (max 1 (int (Math/ceil (/ num-batches 10.0))))
        t-import-start (now-ns)
        batch-times (atom [])]

    (println (format "  Importing %d articles in %d batches of %d..."
                     total-articles num-batches batch-size))

    (doseq [batch-num (range num-batches)]
      (let [batch-start (System/nanoTime)
            offset (* batch-num batch-size)
            articles (mapv #(generate-article (+ offset %)) (range batch-size))]
        (d/transact conn articles)
        (let [batch-time (/ (- (System/nanoTime) batch-start) 1e9)
              rate (/ batch-size batch-time)]
          (swap! batch-times conj {:batch batch-num :time batch-time :rate rate})
          (when (zero? (mod (inc batch-num) print-every))
            (println (format "    Batch %d: %.1f sec (%.0f articles/sec)"
                             batch-num batch-time rate))))))

        (let [t-import-end (now-ns)
          ;; Query benchmarks (run while DB is still connected)
          sample-count (min 1000 total-articles)
          ids (map str (take sample-count (shuffle (range total-articles))))
          categories (map #(str "Category_" %) (range 8))
          size-threshold 20000

          pull-pattern [:article/page-id :article/title :article/byte-size]
          pull-limit 50
          pull-iters 200

          t-lookup-start (System/nanoTime)
          _ (doseq [pid ids]
              (d/q '[:find ?e
                     :in $ ?pid
                     :where [?e :article/page-id ?pid]]
                   @conn pid))
          t-lookup (/ (- (System/nanoTime) t-lookup-start) 1e9)

          t-lookup-pull-start (System/nanoTime)
          _ (doseq [pid ids]
              (let [eid (ffirst (d/q '[:find ?e
                                       :in $ ?pid
                                       :where [?e :article/page-id ?pid]]
                                     @conn pid))]
                (d/pull @conn pull-pattern eid)))
          t-lookup-pull (/ (- (System/nanoTime) t-lookup-pull-start) 1e9)

          t-category-start (System/nanoTime)
          _ (dotimes [_ 500]
              (let [cat (rand-nth categories)]
                (d/q '[:find (count ?e)
                       :in $ ?cat
                       :where [?e :article/category ?cat]]
                     @conn cat)))
          t-category (/ (- (System/nanoTime) t-category-start) 1e9)

          t-bytes-start (System/nanoTime)
          _ (dotimes [_ 500]
              (d/q '[:find (count ?e)
                     :in $ ?t
                     :where [?e :article/byte-size ?s]
                            [(> ?s ?t)]]
                   @conn size-threshold))
          t-bytes (/ (- (System/nanoTime) t-bytes-start) 1e9)

            t-category-pull-start (System/nanoTime)
            _ (dotimes [_ pull-iters]
              (let [cat (rand-nth categories)
                eids (->> (d/q {:query '[:find ?e
                            :in $ ?cat
                            :where [?e :article/category ?cat]]
                         :args [@conn cat]
                         :limit pull-limit})
                      (map first)
                      (into []))]
              (d/pull-many @conn pull-pattern eids)))
            t-category-pull (/ (- (System/nanoTime) t-category-pull-start) 1e9)

            t-bytes-pull-start (System/nanoTime)
            _ (dotimes [_ pull-iters]
              (let [eids (->> (d/q {:query '[:find ?e
                            :in $ ?t
                            :where [?e :article/byte-size ?s]
                                 [(> ?s ?t)]]
                         :args [@conn size-threshold]
                         :limit pull-limit})
                      (map first)
                      (into []))]
              (d/pull-many @conn pull-pattern eids)))
            t-bytes-pull (/ (- (System/nanoTime) t-bytes-pull-start) 1e9)

             t-queries-end (now-ns)
             t-release-start (now-ns)
             _ (d/release conn)
             t-release-end (now-ns)
             t-cleanup-start (now-ns)
             _ (cleanup-dir path)
             t-cleanup-end (now-ns)
             total-elapsed (elapsed-sec t0 t-cleanup-end)
             import-elapsed (elapsed-sec t-import-start t-import-end)
             setup-elapsed (elapsed-sec t0 t-setup-done)
             queries-elapsed (elapsed-sec t-import-end t-queries-end)
             release-elapsed (elapsed-sec t-release-start t-release-end)
             cleanup-elapsed (elapsed-sec t-cleanup-start t-cleanup-end)]
              {:backend :datahike-lmdb
          :total-articles total-articles
          :batch-size batch-size
          :total-time-sec total-elapsed
          :articles-per-sec (/ total-articles total-elapsed)
          :import-time-sec import-elapsed
          :import-articles-per-sec (/ total-articles import-elapsed)
          :batch-times @batch-times
          :phases {:setup-sec setup-elapsed
              :import-sec import-elapsed
              :queries-sec queries-elapsed
              :release-sec release-elapsed
              :cleanup-sec cleanup-elapsed}
          :queries {:lookup-sec t-lookup
               :lookup-qps (/ sample-count t-lookup)
            :lookup-pull-sec t-lookup-pull
            :lookup-pull-qps (/ sample-count t-lookup-pull)
               :category-sec t-category
               :category-qps (/ 500 t-category)
            :byte-threshold-sec t-bytes
            :byte-threshold-qps (/ 500 t-bytes)

            :pull-iters pull-iters
            :pull-limit pull-limit
            :category-pull-sec t-category-pull
            :category-pull-qps (/ pull-iters t-category-pull)
            :category-pull-entities-per-sec (/ (* pull-iters pull-limit) t-category-pull)
            :byte-threshold-pull-sec t-bytes-pull
            :byte-threshold-pull-qps (/ pull-iters t-bytes-pull)
            :byte-threshold-pull-entities-per-sec (/ (* pull-iters pull-limit) t-bytes-pull)}})))

(defn bench-datalevin-wiki
  "Benchmark datalevin with Wikipedia-like data in batches"
  [total-articles batch-size]
  (let [t0 (now-ns)
        path (str base-path "/datalevin-wiki")
        _ (cleanup-dir path)
        conn (dl/get-conn path datalevin-schema)
        t-setup-done (now-ns)
        num-batches (/ total-articles batch-size)
        ;; Print roughly 10 progress lines regardless of total batches
        print-every (max 1 (int (Math/ceil (/ num-batches 10.0))))
        t-import-start (now-ns)
        batch-times (atom [])]

    (println (format "  Importing %d articles in %d batches of %d..."
                     total-articles num-batches batch-size))

    (doseq [batch-num (range num-batches)]
      (let [batch-start (System/nanoTime)
            offset (* batch-num batch-size)
            articles (mapv #(generate-article (+ offset %)) (range batch-size))]
        (dl/transact! conn articles)
        (let [batch-time (/ (- (System/nanoTime) batch-start) 1e9)
              rate (/ batch-size batch-time)]
          (swap! batch-times conj {:batch batch-num :time batch-time :rate rate})
          (when (zero? (mod (inc batch-num) print-every))
            (println (format "    Batch %d: %.1f sec (%.0f articles/sec)"
                             batch-num batch-time rate))))))

        (let [t-import-end (now-ns)
          ;; Query benchmarks (run while DB is still connected)
          db (dl/db conn)
          sample-count (min 1000 total-articles)
          ids (map str (take sample-count (shuffle (range total-articles))))
          categories (map #(str "Category_" %) (range 8))
          size-threshold 20000

          pull-pattern [:article/page-id :article/title :article/byte-size]
          pull-limit 50
          pull-iters 200

          t-lookup-start (System/nanoTime)
          _ (doseq [pid ids]
              (dl/q '[:find ?e
                      :in $ ?pid
                      :where [?e :article/page-id ?pid]]
                    db pid))
          t-lookup (/ (- (System/nanoTime) t-lookup-start) 1e9)

          t-lookup-pull-start (System/nanoTime)
          _ (doseq [pid ids]
              (let [eid (ffirst (dl/q '[:find ?e
                                        :in $ ?pid
                                        :where [?e :article/page-id ?pid]]
                                      db pid))]
                (dl/pull db pull-pattern eid)))
          t-lookup-pull (/ (- (System/nanoTime) t-lookup-pull-start) 1e9)

          t-category-start (System/nanoTime)
          _ (dotimes [_ 500]
              (let [cat (rand-nth categories)]
                (dl/q '[:find (count ?e)
                        :in $ ?cat
                        :where [?e :article/category ?cat]]
                      db cat)))
          t-category (/ (- (System/nanoTime) t-category-start) 1e9)

          t-bytes-start (System/nanoTime)
          _ (dotimes [_ 500]
              (dl/q '[:find (count ?e)
                      :in $ ?t
                      :where [?e :article/byte-size ?s]
                             [(> ?s ?t)]]
                    db size-threshold))
          t-bytes (/ (- (System/nanoTime) t-bytes-start) 1e9)

              t-category-pull-start (System/nanoTime)
              _ (dotimes [_ pull-iters]
              (let [cat (rand-nth categories)
                    q-cat-eids [:find '?e
                        :in '$ '?cat
                        :where ['?e :article/category '?cat]
                        :limit pull-limit]
                    eids (->> (dl/q q-cat-eids db cat)
                  (map first)
                  (into []))]
                (dl/pull-many db pull-pattern eids)))
              t-category-pull (/ (- (System/nanoTime) t-category-pull-start) 1e9)

              t-bytes-pull-start (System/nanoTime)
              _ (dotimes [_ pull-iters]
                (let [q-bytes-eids [:find '?e
                          :in '$ '?t
                          :where ['?e :article/byte-size '?s]
                            ['(> ?s ?t)]
                          :limit pull-limit]
                      eids (->> (dl/q q-bytes-eids db size-threshold)
                  (map first)
                  (into []))]
                (dl/pull-many db pull-pattern eids)))
              t-bytes-pull (/ (- (System/nanoTime) t-bytes-pull-start) 1e9)

            t-queries-end (now-ns)
            t-release-start (now-ns)
            _ (dl/close conn)
            t-release-end (now-ns)
            t-cleanup-start (now-ns)
            _ (cleanup-dir path)
            t-cleanup-end (now-ns)
            total-elapsed (elapsed-sec t0 t-cleanup-end)
            import-elapsed (elapsed-sec t-import-start t-import-end)
            setup-elapsed (elapsed-sec t0 t-setup-done)
            queries-elapsed (elapsed-sec t-import-end t-queries-end)
            release-elapsed (elapsed-sec t-release-start t-release-end)
            cleanup-elapsed (elapsed-sec t-cleanup-start t-cleanup-end)]
          {:backend :datalevin
           :total-articles total-articles
           :batch-size batch-size
           :total-time-sec total-elapsed
           :articles-per-sec (/ total-articles total-elapsed)
           :import-time-sec import-elapsed
           :import-articles-per-sec (/ total-articles import-elapsed)
           :batch-times @batch-times
           :phases {:setup-sec setup-elapsed
              :import-sec import-elapsed
              :queries-sec queries-elapsed
              :release-sec release-elapsed
              :cleanup-sec cleanup-elapsed}
           :queries {:lookup-sec t-lookup
               :lookup-qps (/ sample-count t-lookup)
               :lookup-pull-sec t-lookup-pull
               :lookup-pull-qps (/ sample-count t-lookup-pull)
               :category-sec t-category
               :category-qps (/ 500 t-category)
               :byte-threshold-sec t-bytes
               :byte-threshold-qps (/ 500 t-bytes)

               :pull-iters pull-iters
               :pull-limit pull-limit
               :category-pull-sec t-category-pull
               :category-pull-qps (/ pull-iters t-category-pull)
               :category-pull-entities-per-sec (/ (* pull-iters pull-limit) t-category-pull)
               :byte-threshold-pull-sec t-bytes-pull
               :byte-threshold-pull-qps (/ pull-iters t-bytes-pull)
               :byte-threshold-pull-entities-per-sec (/ (* pull-iters pull-limit) t-bytes-pull)}})))

(defn print-summary [results]
  (println "\n=== Summary ===")
  (doseq [r results]
    (let [{:keys [import-sec queries-sec]} (:phases r)]
      (println (format "%s: %.1f sec total (%.1f art/s avg), import %.2f sec (%.1f art/s), queries %.2f sec"
                       (name (:backend r))
                       (:total-time-sec r)
                       (:articles-per-sec r)
                       import-sec
                       (:import-articles-per-sec r)
                       queries-sec))))

  (println "\n=== Phase Breakdown (sec) ===")
  (doseq [r results]
    (let [{:keys [setup-sec import-sec queries-sec release-sec cleanup-sec]} (:phases r)]
      (println (format "%s: setup %.2f, import %.2f (%.1f art/s), queries %.2f, release %.2f, cleanup %.2f"
                       (name (:backend r))
                       setup-sec
                       import-sec
                       (:import-articles-per-sec r)
                       queries-sec
                       release-sec
                       cleanup-sec))))

  (println "\n=== Performance over time (first vs last 10 batches) ===")
  (doseq [r results]
    (let [times (:batch-times r)
          first-10 (take 10 times)
          last-10 (take-last 10 times)
          avg-first (/ (reduce + (map :rate first-10)) (count first-10))
          avg-last (/ (reduce + (map :rate last-10)) (count last-10))]
      (println (format "%s: first-10 avg %.0f/sec, last-10 avg %.0f/sec (%.0f%% of initial)"
                       (name (:backend r))
                       avg-first
                       avg-last
                       (* 100 (/ avg-last avg-first))))))

  (println "\n=== Query Benchmarks ===")
  (doseq [r results]
    (let [q (:queries r)]
      (println (format "%s: lookup %.3f sec (%.0f QPS), lookup+pull %.3f sec (%.0f QPS), category eq %.3f sec (%.0f QPS), byte-size>%d %.3f sec (%.0f QPS)"
                       (name (:backend r))
                       (:lookup-sec q) (:lookup-qps q)
                       (:lookup-pull-sec q) (:lookup-pull-qps q)
                       (:category-sec q) (:category-qps q)
                       20000 (:byte-threshold-sec q) (:byte-threshold-qps q)))))

  (println "\n=== Query + Pull Benchmarks ===")
  (doseq [r results]
    (let [q (:queries r)
          iters (:pull-iters q)
          limit (:pull-limit q)]
      (println (format "%s: category pull %dx%d %.3f sec (%.0f QPS, %.0f ents/sec), byte-size pull %dx%d %.3f sec (%.0f QPS, %.0f ents/sec)"
                       (name (:backend r))
                       iters limit
                       (:category-pull-sec q)
                       (:category-pull-qps q)
                       (:category-pull-entities-per-sec q)
                       iters limit
                       (:byte-threshold-pull-sec q)
                       (:byte-threshold-pull-qps q)
                       (:byte-threshold-pull-entities-per-sec q))))))

(defn run-wiki-benchmark
  "Run Wikipedia-style benchmark"
  [total-articles batch-size]
  (println "\n=== Wikipedia Article Import Benchmark ===")
  (println (format "Total: %d articles, Batch size: %d" total-articles batch-size))
  (println (format "Path: %s" base-path))
  (println)

  (println "--- Datalevin ---")
  (let [dl-result (bench-datalevin-wiki total-articles batch-size)]
    (println)
    (println "--- Datahike-LMDB ---")
    (let [dh-result (bench-datahike-lmdb-wiki total-articles batch-size)]
      (print-summary [dl-result dh-result])
      [dl-result dh-result])))

(defn -main [& args]
  (let [total (if (seq args) (Integer/parseInt (first args)) 50000)
        batch (if (> (count args) 1) (Integer/parseInt (second args)) 1000)]
    (run-wiki-benchmark total batch)))
