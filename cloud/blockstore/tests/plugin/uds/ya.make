PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/server
    cloud/blockstore/tools/testing/plugintest
    cloud/blockstore/tools/testing/stable-plugin
    cloud/storage/core/tools/testing/unstable-process
    cloud/vm/blockstore

    contrib/ydb/apps/ydbd
)

DATA(
    arcadia/cloud/blockstore/tests/plugin/uds
)

PEERDIR(
    cloud/blockstore/tests/python/lib

    contrib/ydb/tests/library
)

ENV(SANITIZER_TYPE=${SANITIZER_TYPE})

END()
