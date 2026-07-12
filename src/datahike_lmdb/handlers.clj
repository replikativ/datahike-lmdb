(ns datahike-lmdb.handlers
  "Buffer type handlers for PSS types (Datom, Leaf, Branch, PersistentSortedSet).

   These handlers close over settings and storage-atom for decode context,
   allowing them to work without modifying datahike internals.

   Format versioning (backward compatibility): the on-disk node format grew to
   become self-describing (per-node branching-factor + diff-buf-size; Branch also
   carries measure + diff-buf slots) so it can round-trip diff-buffered writes.
   To keep reading databases written before that, node types have TWO tags:
     - v1 (0x41/0x42/0x43): the legacy pre-diff-buf layout — DECODE ONLY.
     - v2 (0x51/0x52/0x53): the current self-describing layout — encode + decode.
   The registry dispatches decode by tag (both layouts read) and encode by class
   (always v2), so old blobs still load, new writes are v2, and a mixed store
   self-heals as the tree rewrites. Legacy nodes are reconstructed with the
   store's Settings (branching-factor from the store, diff-buf-size 0), which is
   exactly what produced them."
  (:require [konserve-lmdb.buffer :as buf]
            [datahike.datom :refer [index-type->cmp-quick]])
  (:import [datahike.datom Datom]
           [org.replikativ.persistent_sorted_set PersistentSortedSet Leaf Branch Slot Settings RefType]
           [java.nio ByteBuffer]
           [java.util UUID Arrays]))

(set! *warn-on-reflection* true)

;;; Per-node Settings reconstruction (self-describing v2 nodes)
;;;
;;; The new PSS carries a diff-buf on Branch nodes, gated by Settings.diffBufSize().
;;; Whether a restored Branch projects its buffered `slots` onto children on read is
;;; decided by that Branch's OWN Settings.diffBufSize() (see Branch.child). The store's
;;; single closed-over Settings is created at connect time with a hardcoded branching
;;; factor and diffBufSize=0, so reconstructing every node from it would silently DISABLE
;;; diff-buf projection on reopen -> buffered writes lost.
;;;
;;; Following the canonical PSS fressian handlers, each v2 Leaf/Branch/root blob carries
;;; its branching-factor + diff-buf-size, and decode reconstructs a per-node Settings from
;;; them (reusing the store's ref-type + nil measure/leaf-processor). Memoized on [bf dbs].

(defn- make-settings-reconstructor
  "Return a memoized (fn [bf dbs] -> Settings) that mirrors the store's ref-type but uses
   the per-node branching-factor + diff-buf-size read from the blob."
  [^Settings base]
  (let [^RefType rt (.refType base)]
    (memoize
     (fn [bf dbs]
       (Settings. (int bf) rt nil nil (int dbs))))))

;;; Type tags in custom range (0x40-0xFF); built-in tags use 0x00-0x1C.
(def ^:const TAG_DATOM     (byte 0x40))
;; v1 — legacy pre-diff-buf layout (decode only)
(def ^:const TAG_LEAF_V1   (byte 0x41))
(def ^:const TAG_BRANCH_V1 (byte 0x42))
(def ^:const TAG_PSS_V1    (byte 0x43))
;; v2 — self-describing, diff-buf-aware layout (encode + decode)
(def ^:const TAG_LEAF_V2   (byte 0x51))
(def ^:const TAG_BRANCH_V2 (byte 0x52))
(def ^:const TAG_PSS_V2    (byte 0x53))

(defn- decode-only [what]
  (throw (ex-info (str "legacy datahike-lmdb " what " handler is decode-only; writes use v2") {})))

;;; Datom Handler (unchanged; no format version)

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

;;; ── v2 handlers (self-describing; used for all encoding) ────────────────────

(defn create-leaf-handler
  "v2 Leaf handler. Self-describing: serializes branching-factor + diff-buf-size so a
   restored Leaf's Settings match the store that produced it."
  [^Settings settings]
  (let [mk-settings (make-settings-reconstructor settings)]
    (reify buf/ITypeHandler
      (type-tag [_] TAG_LEAF_V2)
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
  "v2 Branch handler. Serializes (and restores) the diff-buf `slots` map so a Branch
   carrying buffered writes survives an LMDB round-trip, plus branching-factor +
   diff-buf-size so the restored Branch's projection fires."
  [^Settings settings]
  (let [mk-settings (make-settings-reconstructor settings)]
    (reify buf/ITypeHandler
      (type-tag [_] TAG_BRANCH_V2)
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

(defn create-pss-handler
  "v2 PersistentSortedSet (root) handler. Closes over settings and storage-atom."
  [^Settings settings storage-atom]
  (let [mk-settings (make-settings-reconstructor settings)]
    (reify buf/ITypeHandler
      (type-tag [_] TAG_PSS_V2)
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
              cmp (index-type->cmp-quick (:index-type pss-meta) false)]
          ;; Always reconstruct a REAL PersistentSortedSet — never a stub. `storage` may be
          ;; nil for an LMDB tiered frontend (the atom is nested, not datahike-filled); that's
          ;; fine — datahike binds the connection's storage onto the root at materialization
          ;; (writing.cljc `attach` / index/with-storage) and PSS threads it to children via
          ;; child(storage, idx). Returning a stub (a plain map) defeats `attach`.
          (PersistentSortedSet. pss-meta cmp address storage nil cnt (mk-settings bf dbs) 0))))))

;;; ── v1 legacy handlers (decode pre-diff-buf databases) ──────────────────────
;;; Old layout had no per-node branching-factor / diff-buf-size / measure / slots.
;;; Reconstruct with the store's Settings (bf from the store, diff-buf-size 0), which is
;;; what wrote them. Encode is never invoked (the registry encodes by class -> v2).

(defn create-leaf-handler-v1 [^Settings settings]
  (reify buf/ITypeHandler
    (type-tag [_] TAG_LEAF_V1)
    (type-class [_] Leaf)
    (encode-type [_ _ _ _] (decode-only "Leaf"))
    (decode-type [_ buf decode-fn]
      (let [^ByteBuffer b buf
            len (.getInt b)
            ^objects keys (make-array Object len)]
        (dotimes [i len]
          (aset keys i (decode-fn b)))
        (Leaf. len keys settings)))))

(defn create-branch-handler-v1 [^Settings settings]
  (reify buf/ITypeHandler
    (type-tag [_] TAG_BRANCH_V1)
    (type-class [_] Branch)
    (encode-type [_ _ _ _] (decode-only "Branch"))
    (decode-type [_ buf decode-fn]
      (let [^ByteBuffer b buf
            level (.getInt b)
            len (.getInt b)
            subtree-count (.getLong b)
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
        (let [b* (Branch. (int level) (Arrays/asList keys) (Arrays/asList addresses) settings)]
          (set! (.-_subtreeCount b*) (long subtree-count))
          b*)))))

(defn create-pss-handler-v1 [^Settings settings storage-atom]
  (reify buf/ITypeHandler
    (type-tag [_] TAG_PSS_V1)
    (type-class [_] PersistentSortedSet)
    (encode-type [_ _ _ _] (decode-only "PersistentSortedSet"))
    (decode-type [_ buf decode-fn]
      (let [^ByteBuffer b buf
            pss-meta (decode-fn b)
            msb (.getLong b)
            lsb (.getLong b)
            address (UUID. msb lsb)
            cnt (.getInt b)
            storage @storage-atom
            cmp (index-type->cmp-quick (:index-type pss-meta) false)]
        (PersistentSortedSet. pss-meta cmp address storage nil cnt settings 0)))))

;;; Factory function

(defn create-pss-handlers
  "All PSS type handlers for a store's `settings` + `storage-atom`.

   v1 (legacy) handlers are listed BEFORE v2 so the registry's encode dispatch
   (by class, last-write-wins) resolves to v2, while decode dispatch (by tag)
   keeps both — old-format blobs still read, new writes are v2."
  [^Settings settings storage-atom]
  [(create-datom-handler)
   (create-leaf-handler-v1 settings)
   (create-branch-handler-v1 settings)
   (create-pss-handler-v1 settings storage-atom)
   (create-leaf-handler settings)
   (create-branch-handler settings)
   (create-pss-handler settings storage-atom)])
