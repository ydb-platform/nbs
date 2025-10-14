PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/large.inc)

TEST_SRCS(test.py)

DEPENDS(
    cloud/blockstore/tools/testing/NBSNEBIUS-972
)

DATA(
)

PEERDIR(
)

END()
