PY3TEST()

ENV(YDB_USE_IN_MEMORY_PDISKS=true)

ENV(YDB_STATIC_PDISK_SIZE=34359738368)
ENV(YDB_DYNAMIC_PDISK_SIZE=34359738368)

RESOURCE(dynamic_storage_pools.json dynamic_storage_pools)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/ya.make.inc)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

PEERDIR(
    contrib/ydb/core/protos
    contrib/python/importlib-resources
    contrib/python/protobuf
)

TEST_SRCS(
    test.py
)

DATA(
    arcadia/cloud/blockstore/tests/loadtest/compaction
)

END()
