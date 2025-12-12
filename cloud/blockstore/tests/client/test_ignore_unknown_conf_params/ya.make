PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

TEST_SRCS(
    test_ignore_unknown_conf_params.py
)

DEPENDS(
    cloud/blockstore/apps/client
)

PEERDIR(
    cloud/blockstore/tests/python/lib
)

END()
