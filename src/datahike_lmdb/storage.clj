(ns datahike-lmdb.storage
  "IStorage implementation for PSS that uses konserve for persistence.

   BufferedStorage provides:
   - LRU cache for frequently accessed nodes
   - Pending writes batching
   - Uses konserve k/get for reads (goes through LMDBStore's type handlers)"
  (:require [clojure.core.cache :as cache]
            [clojure.core.cache.wrapped :as wrapped]
            [konserve.core :as k]
            [hasch.core :refer [uuid]]
            [taoensso.timbre :refer [trace]])
  (:import [me.tonsky.persistent_sorted_set IStorage ANode Leaf Branch]
           [java.util UUID]))

(set! *warn-on-reflection* true)

(def ^:const +default-cache-size+ 1000)

(defn- gen-address
  "Generate address for a PSS node."
  [^ANode node crypto-hash?]
  (if crypto-hash?
    (if (instance? Branch node)
      (uuid (vec (.addresses ^Branch node)))
      (uuid (mapv (comp vec seq) (.keys node))))
    (uuid)))

(defrecord BufferedStorage [store cache stats pending-writes crypto-hash?]
  IStorage
  (store [_ node]
    (swap! stats update :writes inc)
    (let [address (gen-address node crypto-hash?)]
      (trace "BufferedStorage.store:" address)
      (swap! pending-writes conj [address node])
      (wrapped/miss cache address node)
      address))

  (accessed [_ address]
    (trace "BufferedStorage.accessed:" address)
    (swap! stats update :accessed inc)
    (wrapped/hit cache address)
    nil)

  (restore [_ address]
    (trace "BufferedStorage.restore:" address)
    (if-let [cached (wrapped/lookup cache address)]
      (do
        (swap! stats update :cache-hits inc)
        cached)
      (let [node (k/get store address nil {:sync? true})]
        (when (nil? node)
          (throw (ex-info "Node not found in storage"
                          {:type :node-not-found
                           :address address})))
        (swap! stats update :reads inc)
        (wrapped/miss cache address node)
        node))))

;;; Public API

(defn create-buffered-storage
  "Create a BufferedStorage instance.

   Arguments:
     store - The underlying konserve store (LMDBStore)

   Options:
     :cache-size - LRU cache size (default 1000)
     :crypto-hash? - Use content-based addressing (default false)"
  [store & {:keys [cache-size crypto-hash?]
            :or {cache-size +default-cache-size+
                 crypto-hash? false}}]
  (->BufferedStorage
   store
   (atom (cache/lru-cache-factory {} :threshold cache-size))
   (atom {:writes 0 :reads 0 :accessed 0 :cache-hits 0})
   (atom [])
   crypto-hash?))

(defn get-pending-writes
  "Get pending writes without clearing."
  [^BufferedStorage storage]
  @(:pending-writes storage))

(defn clear-pending-writes!
  "Clear pending writes and return them."
  [^BufferedStorage storage]
  (let [pending-atom (:pending-writes storage)
        writes (atom [])]
    (swap! pending-atom (fn [old] (reset! writes old) []))
    @writes))

(defn flush-pending-writes!
  "Flush all pending writes to the store using batch operations.
   Returns the number of writes."
  [^BufferedStorage storage]
  (let [store (:store storage)
        pending (clear-pending-writes! storage)
        write-count (count pending)]
    (when (pos? write-count)
      (trace "BufferedStorage.flush:" write-count "nodes")
      ;; Use multi-assoc for batch writes in a single transaction
      (k/multi-assoc store (into {} pending) {:sync? true}))
    write-count))

(defn storage-stats
  "Get storage statistics."
  [^BufferedStorage storage]
  @(:stats storage))

(defn reset-stats!
  "Reset storage statistics."
  [^BufferedStorage storage]
  (reset! (:stats storage) {:writes 0 :reads 0 :accessed 0 :cache-hits 0}))
