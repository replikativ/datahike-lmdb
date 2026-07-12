(ns datahike-lmdb.handlers
  "Buffer type handlers for PSS types (Datom, Leaf, Branch, PersistentSortedSet).

   These handlers close over settings and storage-atom for decode context,
   allowing them to work without modifying datahike internals."
  (:require [konserve-lmdb.buffer :as buf]
            [datahike.datom :refer [index-type->cmp-quick]])
  (:import [datahike.datom Datom]
           [org.replikativ.persistent_sorted_set PersistentSortedSet Leaf Branch Slot Settings RefType]
           [java.nio ByteBuffer]
           [java.util UUID Arrays]))

(set! *warn-on-reflection* true)

;;; Per-node Settings reconstruction (self-describing nodes)
;;;
;;; The new PSS carries a diff-buf on Branch nodes, gated by Settings.diffBufSize().
;;; Whether a restored Branch projects its buffered `slots` onto children on read is
;;; decided by that Branch's OWN Settings.diffBufSize() (see Branch.child). The store's
;;; single closed-over Settings is created at connect time with a hardcoded branching
;;; factor and diffBufSize=0, so reconstructing every node from it would silently DISABLE
;;; diff-buf projection on reopen -> buffered writes lost.
;;;
;;; Following the canonical PSS fressian handlers, we make each node SELF-DESCRIBING:
;;; every Leaf/Branch/root blob additionally carries its branching-factor + diff-buf-size,
;;; and decode reconstructs a per-node Settings from them (reusing the store's ref-type +
;;; nil measure/leaf-processor). Memoized on [bf dbs] to avoid per-node allocation.

(defn- make-settings-reconstructor
  "Return a memoized (fn [bf dbs] -> Settings) that mirrors the store's ref-type but uses
   the per-node branching-factor + diff-buf-size read from the blob."
  [^Settings base]
  (let [^RefType rt (.refType base)]
    (memoize
     (fn [bf dbs]
       (Settings. (int bf) rt nil nil (int dbs))))))

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
  "Create handler for Leaf type. Closes over settings.

   Self-describing: serializes branching-factor + diff-buf-size so a restored Leaf's
   Settings match the store that produced it (a single-leaf root that is reopened and
   mutated must split at the right branching factor / buffer correctly)."
  [^Settings settings]
  (let [mk-settings (make-settings-reconstructor settings)]
    (reify buf/ITypeHandler
      (type-tag [_] TAG_LEAF)
      (type-class [_] Leaf)
      (encode-type [_ buf leaf _encode-fn]
        (let [^ByteBuffer b buf
              ^Leaf l leaf
              len (.-_len l)
              keys (.-_keys l)
              ^Settings s (.-_settings l)]
          (.putInt b len)
          (.putInt b (.branchingFactor s))
          (.putInt b (.diffBufSize s))
          (dotimes [i len]
            (_encode-fn b (aget keys i)))))
      (decode-type [_ buf decode-fn]
        (let [^ByteBuffer b buf
              len (.getInt b)
              bf (.getInt b)
              dbs (.getInt b)
              ^objects keys (make-array Object len)]
          (dotimes [i len]
            (aset keys i (decode-fn b)))
          (Leaf. len keys (mk-settings bf dbs)))))))

;;; Branch Handler (needs Settings)

(defn- attach-slots!
  "Rebuild a restored Branch's diff-buf slots from the stored {idx -> entry} map and
   install them. Mirrors org.replikativ.persistent-sorted-set.fressian/attach-slots!:
   the anchor is RE-DERIVED as addresses[idx] (never stored), and :max-key is ignored on
   restore (count/measure/diff + derived anchor fully determine the Slot). Installed with
   entries = Branch/BUF_LAZY so the buffered-entry count is derived from the slots on first
   read."
  [^Branch b ^objects addresses slots]
  (let [arr (object-array (alength addresses))]
    (doseq [[idx entry] slots]
      (let [i (int idx)]
        (aset arr i (Slot. (:diff entry) (long (:count entry)) (:measure entry)
                           (aget addresses i)))))
    (.installSlots b arr Branch/BUF_LAZY)))

(defn create-branch-handler
  "Create handler for Branch type. Closes over settings.

   Serializes (and restores) the diff-buf `slots` map so a Branch carrying buffered writes
   survives an LMDB round-trip; without it buffered diffs would be silently dropped on
   reopen -> data corruption. Also self-describing (branching-factor + diff-buf-size) so
   the restored Branch's Settings.diffBufSize() > 0 and projection actually fires."
  [^Settings settings]
  (let [mk-settings (make-settings-reconstructor settings)]
    (reify buf/ITypeHandler
      (type-tag [_] TAG_BRANCH)
      (type-class [_] Branch)
      (encode-type [_ buf branch encode-fn]
        (let [^ByteBuffer b buf
              ^Branch br branch
              level (.-_level br)
              len (.-_len br)
              keys (.-_keys br)
              ^java.util.List addrs (.addresses br)
              ^Settings s (.-_settings br)
              subtree-count (.subtreeCount br)]
          (.putInt b level)
          (.putInt b len)
          (.putLong b subtree-count)
          (.putInt b (.branchingFactor s))
          (.putInt b (.diffBufSize s))
          (dotimes [i len]
            (encode-fn b (aget keys i)))
          (dotimes [i len]
            (let [^UUID addr (.get addrs i)]
              (if addr
                (do
                  (.putLong b (.getMostSignificantBits addr))
                  (.putLong b (.getLeastSignificantBits addr)))
                (do
                  (.putLong b 0)
                  (.putLong b 0)))))
          ;; measure (nil for datahike) then the diff-buf slots (nil when buffer empty/off).
          ;; Both go through the generic registry codec, which round-trips Clojure maps/sets/
          ;; keywords/longs and Datoms (via the Datom handler) — the leaf-diff storage form
          ;; {:absent #{datom...} :present #{datom...}} therefore round-trips.
          (encode-fn b (.-_measure br))
          (encode-fn b (.slotsForStorage br))))
      (decode-type [_ buf decode-fn]
        (let [^ByteBuffer b buf
              level (.getInt b)
              len (.getInt b)
              subtree-count (.getLong b)
              bf (.getInt b)
              dbs (.getInt b)
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
          (let [measure (decode-fn b)
                slots (decode-fn b)
                ^Settings s (mk-settings bf dbs)
                b* (Branch. (int level) (Arrays/asList keys) (Arrays/asList addresses) s)]
            (set! (.-_subtreeCount b*) (long subtree-count))
            (when (some? measure) (set! (.-_measure b*) measure))
            (when slots (attach-slots! b* addresses slots))
            b*))))))

;;; PersistentSortedSet Handler (needs Settings and Storage)

(defn create-pss-handler
  "Create handler for PersistentSortedSet type.
   Closes over settings and storage-atom for decode context."
  [^Settings settings storage-atom]
  (let [mk-settings (make-settings-reconstructor settings)]
    (reify buf/ITypeHandler
      (type-tag [_] TAG_PSS)
      (type-class [_] PersistentSortedSet)
      (encode-type [_ buf pss encode-fn]
        (let [^ByteBuffer b buf
              ^PersistentSortedSet p pss
              ^Settings s (.-_settings p)]
          (when (nil? (.-_address p))
            (throw (ex-info "PersistentSortedSet must be flushed before encoding" {:pss p})))
          (encode-fn b (meta p))
          (let [^UUID addr (.-_address p)]
            (.putLong b (.getMostSignificantBits addr))
            (.putLong b (.getLeastSignificantBits addr)))
          (.putInt b (count p))
          ;; Self-describing: the root's branching-factor + diff-buf-size, so a reopened
          ;; root that is then mutated buffers / splits with its original settings.
          (.putInt b (.branchingFactor s))
          (.putInt b (.diffBufSize s))))
      (decode-type [_ buf decode-fn]
        (let [^ByteBuffer b buf
              pss-meta (decode-fn b)
              msb (.getLong b)
              lsb (.getLong b)
              address (UUID. msb lsb)
              cnt (.getInt b)
              bf (.getInt b)
              dbs (.getInt b)
              storage @storage-atom
              index-type (:index-type pss-meta)
              cmp (index-type->cmp-quick index-type false)]
          ;; Always reconstruct a REAL PersistentSortedSet — never a stub. `storage`
          ;; may be nil here: for a direct :lmdb store datahike fills the store's
          ;; storage-atom, but for an LMDB *tiered frontend* the atom is nested and
          ;; not datahike-filled. That's fine — the root carries only its address,
          ;; and datahike binds the connection's tier-wide storage onto it at
          ;; materialization (writing.cljc `attach` / index/with-storage). PSS then
          ;; threads that storage to children via child(storage, idx); restored
          ;; children are bare nodes, so no per-node storage is needed. Returning a
          ;; stub (a plain map, not a PSS) here defeats `attach` — it only re-binds
          ;; PersistentSortedSet roots — which is what broke the tiered case.
          (PersistentSortedSet. pss-meta cmp address storage nil cnt (mk-settings bf dbs) 0))))))

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
