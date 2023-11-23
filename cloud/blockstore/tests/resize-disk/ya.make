PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(1)

PEERDIR(
    cloud/blockstore/tests/python/lib
)

TEST_SRCS(
    test.py
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/service-local/service-local.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/endpoint/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/resize-disk/resize-disk.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/qemu.inc)

END()
