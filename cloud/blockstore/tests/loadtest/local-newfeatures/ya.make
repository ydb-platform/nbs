PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/ya.make.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/storage/core/tools/testing/unstable-process
)

DATA(
    arcadia/cloud/blockstore/tests/loadtest/local-newfeatures
)

END()
