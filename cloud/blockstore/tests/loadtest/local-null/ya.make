PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/large.inc)

TEST_SRCS(
    test.py
)

# fixing crashes under tsan in NBS-4155
IF(SANITIZER_TYPE == "thread")
    TAG(
        ya:not_autocheck
        ya:manual
    )
ENDIF()

USE_RECIPE(cloud/blockstore/tests/recipes/local-null/local-null-recipe)

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/server_lightweight
    cloud/blockstore/tests/recipes/local-null
    cloud/blockstore/tools/testing/loadtest/bin
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
    arcadia/cloud/blockstore/tests/loadtest/local-null
)

END()
