# Legacy backward-compatibility fixture

`test/resources/legacy-buffer-store/data.mdb` is a real datahike-lmdb store
written by the **pre-boring** stack — the buffer codec (konserve-lmdb 0.1.16,
PSS 0.4.137, datahike 0.8.1729). It exists so `reads-pre-boring-buffer-store`
(in `test/datahike_lmdb/store_test.clj`) proves an old store still reads under
the current boring config: konserve-lmdb routes buffer blobs to the legacy
`:type-handlers` while the store is opened with the boring `:registry`, and the
v1 keyspace still resolves.

Contents: schema `:item/name` (string, cardinality-one) with three entities —
`"Widget"`, `"Gadget"`, `"Gizmo"`. Store id `#uuid "0f1e2d3c-4b5a-6789-0abc-def012345678"`.

## Regenerating it

Generated from this repo's own pre-boring commit `33f8286` via a worktree, so
the bytes are authentic rather than synthesized:

```bash
git worktree add ../datahike-lmdb-legacy 33f8286
cd ../datahike-lmdb-legacy
clojure -J--enable-native-access=ALL-UNNAMED -M -e '
  (require (quote [datahike.api :as d]) (quote datahike-lmdb.core))
  (def cfg {:store {:backend :lmdb :path "/tmp/legacy-gen"
                    :id #uuid "0f1e2d3c-4b5a-6789-0abc-def012345678"
                    :map-size (* 8 1024 1024)}
            :schema-flexibility :write :keep-history? false})
  (d/create-database cfg)
  (let [c (d/connect cfg)]
    (d/transact c [{:db/ident :item/name :db/valueType :db.type/string
                    :db/cardinality :db.cardinality/one}])
    (d/transact c [{:item/name "Widget"} {:item/name "Gadget"} {:item/name "Gizmo"}])
    (d/release c))'
cp /tmp/legacy-gen/data.mdb ../datahike-lmdb/test/resources/legacy-buffer-store/data.mdb
git worktree remove ../datahike-lmdb-legacy
```

`lock.mdb` is intentionally not committed — LMDB recreates it on open.
