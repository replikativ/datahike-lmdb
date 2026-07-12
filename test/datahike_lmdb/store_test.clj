(ns datahike-lmdb.store-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [datahike.api :as d]
            [konserve.store :as ks]
            [konserve-lmdb.buffer :as buf]
            [datahike-lmdb.handlers :as handlers]
            [datahike-lmdb.core])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet Leaf Branch Settings RefType]))

(def test-path "/tmp/datahike-lmdb-test")
(def test-id (java.util.UUID/randomUUID))

(defn cleanup-fixture [f]
  (try
    (d/delete-database {:store {:backend :lmdb :path test-path :id test-id}})
    (catch Exception _))
  (f)
  (try
    (d/delete-database {:store {:backend :lmdb :path test-path :id test-id}})
    (catch Exception _)))

(use-fixtures :each cleanup-fixture)

(deftest lmdb-backend-registered
  (testing "LMDB backend is registered in konserve.store"
    (is (contains? (set (keys (methods ks/-create-store))) :lmdb))
    (is (contains? (set (keys (methods ks/-connect-store))) :lmdb))
    (is (contains? (set (keys (methods ks/-delete-store))) :lmdb))))

(deftest create-and-connect-lmdb-database
  (testing "Can create and connect to LMDB database"
    (let [cfg {:store {:backend :lmdb :path test-path :id test-id}
               :schema-flexibility :write
               :keep-history? false}]
      (is (d/create-database cfg))
      (let [conn (d/connect cfg)]
        (is (some? conn))
        (d/release conn)))))

(deftest transact-and-query-lmdb
  (testing "Can transact and query data"
    (let [cfg {:store {:backend :lmdb :path test-path :id test-id}
               :schema-flexibility :write
               :keep-history? false}]
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        ;; Add schema
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :person/age
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}])

        ;; Add data
        (d/transact conn [{:person/name "Alice" :person/age 30}
                          {:person/name "Bob" :person/age 25}])

        ;; Query
        (let [result (d/q '[:find ?n ?a
                            :where [?e :person/name ?n]
                            [?e :person/age ?a]]
                          @conn)]
          (is (= #{["Alice" 30] ["Bob" 25]} result)))

        (d/release conn)))))

(deftest persistence-across-reconnect
  (testing "Data persists across reconnect"
    (let [cfg {:store {:backend :lmdb :path test-path :id test-id}
               :schema-flexibility :write
               :keep-history? false}]
      ;; Create and write
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn [{:db/ident :item/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:item/name "Widget"}])
        (d/release conn))

      ;; Reconnect and read
      (let [conn (d/connect cfg)
            result (d/q '[:find ?n
                          :where [?e :item/name ?n]]
                        @conn)]
        (is (= #{["Widget"]} result))
        (d/release conn)))))

(deftest dual-format-handler-dispatch
  (testing "registry keeps legacy (v1) tags for DECODE and encodes as v2 (backward compat)"
    (let [settings (Settings. 512 RefType/SOFT)
          reg      (buf/create-handler-registry
                    (handlers/create-pss-handlers settings (atom nil)) nil)
          by-tag   (:by-tag reg)]
      (testing "all node tags — legacy v1 (0x41-0x43) AND v2 (0x51-0x53) — are decodable"
        (doseq [t [0x40 0x41 0x42 0x43 0x51 0x52 0x53]]
          (is (some? (.get ^java.util.HashMap by-tag (int t)))
              (format "tag 0x%02x must route to a decode handler" t))))
      (testing "encode (dispatch by class) resolves to the v2 handler for every node type"
        (is (= handlers/TAG_LEAF_V2   (buf/type-tag (buf/registry-get-handler-for-class reg Leaf))))
        (is (= handlers/TAG_BRANCH_V2 (buf/type-tag (buf/registry-get-handler-for-class reg Branch))))
        (is (= handlers/TAG_PSS_V2    (buf/type-tag (buf/registry-get-handler-for-class reg PersistentSortedSet))))))))
