(ns datahike-lmdb.core
  "LMDB storage backend for Datahike.

   Usage:
     (require '[datahike-lmdb.core])  ;; registers :lmdb backend
     (require '[datahike.api :as d])

     (d/create-database {:store {:backend :lmdb
                                 :path \"/path/to/db\"
                                 :id (java.util.UUID/randomUUID)}})
     (def conn (d/connect {:store {:backend :lmdb
                                   :path \"/path/to/db\"
                                   :id #uuid \"...\"}}))

   Options:
     :path       - Directory path for LMDB data files (required)
     :id         - Store UUID (required)
     :map-size   - LMDB map size in bytes (default 1GB)"
  (:require [konserve.store :as ks]
            [konserve.utils :refer [async+sync *default-sync-translation*]]
            [konserve-lmdb.store :as lmdb]
            [konserve-lmdb.buffer :as buf]
            [datahike-lmdb.handlers :as handlers]
            [datahike-lmdb.storage :as storage]
            [superv.async :refer [go-try-]])
  (:import [me.tonsky.persistent_sorted_set Settings]))

(def ^:const +default-branching-factor+ 512)

(defn- create-lmdb-store
  "Create an LMDBStore with PSS handlers configured.
   Returns store with :storage-atom field for datahike to populate."
  [{:keys [path map-size flags]}]
  (let [settings (Settings. +default-branching-factor+ nil)
        storage-atom (atom nil)
        pss-handlers (handlers/create-pss-handlers settings storage-atom)
        ;; Create handler registry with our custom PSS handlers
        type-handlers (buf/create-handler-registry pss-handlers nil)
        ;; Connect store with custom handlers
        store (lmdb/connect-store path
                                   :map-size (or map-size lmdb/+default-map-size+)
                                   :flags (or flags 0)
                                   :type-handlers type-handlers)]
    ;; Store the storage-atom so datahike's add-konserve-handlers can find it
    (assoc store :storage-atom storage-atom)))

;; Override konserve.store multimethods to set up PSS handlers
;; These override konserve-lmdb's implementations since this ns is loaded after

(defmethod ks/-create-store :lmdb
  [{:keys [path] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               ;; Check if store already exists
               (when (.exists (clojure.java.io/file path))
                 (throw (ex-info (str "LMDB store already exists at path: " path)
                                 {:path path :config config})))
               (create-lmdb-store config))))

(defmethod ks/-connect-store :lmdb
  [{:keys [path] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               ;; Check if store exists
               (when-not (.exists (clojure.java.io/file path))
                 (throw (ex-info (str "LMDB store does not exist at path: " path)
                                 {:path path :config config})))
               (create-lmdb-store config))))

(defmethod ks/-store-exists? :lmdb
  [{:keys [path]} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (.exists (clojure.java.io/file path)))))

(defmethod ks/-delete-store :lmdb
  [{:keys [path]} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (lmdb/delete-store path))))

(defmethod ks/-release-store :lmdb
  [_config store opts]
  ;; Flush any pending writes before releasing
  (when-let [storage (:storage store)]
    (storage/flush-pending-writes! storage))
  (lmdb/release-store store)
  (if (:sync? opts) nil (go-try- nil)))
