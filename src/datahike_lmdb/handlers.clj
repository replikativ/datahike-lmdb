(ns datahike-lmdb.handlers
  "Buffer type handlers for PSS types (Datom, Leaf, Branch, PersistentSortedSet).

   These handlers close over settings and storage-atom for decode context,
   allowing them to work without modifying datahike internals."
  (:require [konserve-lmdb.buffer :as buf]
            [datahike.datom :refer [index-type->cmp-quick]])
  (:import [datahike.datom Datom]
           [me.tonsky.persistent_sorted_set PersistentSortedSet Leaf Branch Settings]
           [java.nio ByteBuffer]
           [java.util UUID]))

(set! *warn-on-reflection* true)

;;; Type tags in custom range (0x40-0xFF)
;;; Built-in tags use 0x00-0x1C, so we start at 0x40 for safety
(def ^:const TAG_DATOM  (byte 0x40))
(def ^:const TAG_LEAF   (byte 0x41))
(def ^:const TAG_BRANCH (byte 0x42))
(def ^:const TAG_PSS    (byte 0x43))

;;; Datom Handler (no context needed)

(defn create-datom-handler
  "Create handler for Datom type."
  []
  (reify buf/ITypeHandler
    (type-tag [_] TAG_DATOM)
    (type-class [_] Datom)
    (encode-type [_ buf datom encode-fn]
      (let [^ByteBuffer b buf
            ^Datom d datom]
        (.putLong b (.-e d))
        (encode-fn b (.-a d))
        (encode-fn b (.-v d))
        (.putLong b (.-tx d))))
    (decode-type [_ buf decode-fn]
      (let [^ByteBuffer b buf
            e (.getLong b)
            a (decode-fn b)
            v (decode-fn b)
            tx (.getLong b)]
        (Datom. e a v tx 0)))))

;;; Leaf Handler (needs Settings)

(defn create-leaf-handler
  "Create handler for Leaf type. Closes over settings."
  [^Settings settings]
  (reify buf/ITypeHandler
    (type-tag [_] TAG_LEAF)
    (type-class [_] Leaf)
    (encode-type [_ buf leaf encode-fn]
      (let [^ByteBuffer b buf
            ^Leaf l leaf
            len (.-_len l)
            keys (.-_keys l)]
        (.putInt b len)
        (dotimes [i len]
          (encode-fn b (aget keys i)))))
    (decode-type [_ buf decode-fn]
      (let [^ByteBuffer b buf
            len (.getInt b)
            ^objects keys (make-array Object len)]
        (dotimes [i len]
          (aset keys i (decode-fn b)))
        (Leaf. len keys settings)))))

;;; Branch Handler (needs Settings)

(defn create-branch-handler
  "Create handler for Branch type. Closes over settings."
  [^Settings settings]
  (reify buf/ITypeHandler
    (type-tag [_] TAG_BRANCH)
    (type-class [_] Branch)
    (encode-type [_ buf branch encode-fn]
      (let [^ByteBuffer b buf
            ^Branch br branch
            level (.-_level br)
            len (.-_len br)
            keys (.-_keys br)
            addresses (.-_addresses br)]
        (.putInt b level)
        (.putInt b len)
        (dotimes [i len]
          (encode-fn b (aget keys i)))
        (dotimes [i len]
          (let [^UUID addr (aget addresses i)]
            (if addr
              (do
                (.putLong b (.getMostSignificantBits addr))
                (.putLong b (.getLeastSignificantBits addr)))
              (do
                (.putLong b 0)
                (.putLong b 0)))))))
    (decode-type [_ buf decode-fn]
      (let [^ByteBuffer b buf
            level (.getInt b)
            len (.getInt b)
            ^objects keys (make-array Object len)
            ^objects addresses (make-array Object len)]
        (dotimes [i len]
          (aset keys i (decode-fn b)))
        (dotimes [i len]
          (let [msb (.getLong b)
                lsb (.getLong b)]
            (if (and (zero? msb) (zero? lsb))
              (aset addresses i nil)
              (aset addresses i (UUID. msb lsb)))))
        (Branch. level len keys addresses nil settings)))))

;;; PersistentSortedSet Handler (needs Settings and Storage)

(defn create-pss-handler
  "Create handler for PersistentSortedSet type.
   Closes over settings and storage-atom for decode context."
  [^Settings settings storage-atom]
  (reify buf/ITypeHandler
    (type-tag [_] TAG_PSS)
    (type-class [_] PersistentSortedSet)
    (encode-type [_ buf pss encode-fn]
      (let [^ByteBuffer b buf
            ^PersistentSortedSet p pss]
        (when (nil? (.-_address p))
          (throw (ex-info "PersistentSortedSet must be flushed before encoding" {:pss p})))
        (encode-fn b (meta p))
        (let [^UUID addr (.-_address p)]
          (.putLong b (.getMostSignificantBits addr))
          (.putLong b (.getLeastSignificantBits addr)))
        (.putInt b (count p))))
    (decode-type [_ buf decode-fn]
      (let [^ByteBuffer b buf
            pss-meta (decode-fn b)
            msb (.getLong b)
            lsb (.getLong b)
            address (UUID. msb lsb)
            cnt (.getInt b)
            storage @storage-atom]
        (if storage
          ;; Full reconstruction with storage
          (let [index-type (:index-type pss-meta)
                cmp (index-type->cmp-quick index-type false)]
            (PersistentSortedSet. pss-meta cmp address storage nil cnt settings 0))
          ;; Return stub for later reconstruction (shouldn't happen normally)
          {:pss/stub true
           :pss/meta pss-meta
           :pss/address address
           :pss/count cnt})))))

;;; Factory function

(defn create-pss-handlers
  "Create all PSS type handlers with given settings and storage-atom.

   The handlers close over these references, allowing decode to access
   the storage after it's been created."
  [^Settings settings storage-atom]
  [(create-datom-handler)
   (create-leaf-handler settings)
   (create-branch-handler settings)
   (create-pss-handler settings storage-atom)])
