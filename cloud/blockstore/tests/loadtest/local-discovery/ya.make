PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/ya.make.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/tools/testing/fake-conductor
    cloud/blockstore/tools/testing/fake-nbs
)

DATA(
    arcadia/cloud/blockstore/tests/loadtest/local-discovery
)

END()
