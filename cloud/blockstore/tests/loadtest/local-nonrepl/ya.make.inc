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

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/tools/testing/loadtest/bin

    cloud/storage/core/tools/testing/unstable-process

    ydb/apps/ydbd
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
    arcadia/cloud/blockstore/tests/loadtest/local-nonrepl
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib

    ydb/core/protos
    ydb/tests/library
)

REQUIREMENTS(
    ram_disk:32
    cpu:16
    ram:96
    container:5897560351  # container with tcp_tw_reuse = 1
)

FORK_SUBTESTS()
SPLIT_FACTOR(4)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/fake-root-kms/recipe.inc)
