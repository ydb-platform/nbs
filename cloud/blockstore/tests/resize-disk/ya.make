PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(1)

PEERDIR(
    cloud/blockstore/public/sdk/python/client
    cloud/blockstore/tests/python/lib
)

TEST_SRCS(
    test.py
)

SET(LOCAL_KIKIMR_SERVER_COUNT 2)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/local-kikimr/local-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/endpoint/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/qemu.inc)

END()
