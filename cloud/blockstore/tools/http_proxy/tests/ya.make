PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

USE_RECIPE(cloud/blockstore/tests/recipes/local-null/local-null-recipe)

DEPENDS(
    cloud/blockstore/apps/server_lightweight
    cloud/blockstore/tests/recipes/local-null
    cloud/blockstore/tools/http_proxy
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

PEERDIR(
    contrib/ydb/tests/library
    cloud/blockstore/pylibs/ydb/tests/library

    cloud/blockstore/tests/python/lib
)

TEST_SRCS(
    test.py
)

END()
