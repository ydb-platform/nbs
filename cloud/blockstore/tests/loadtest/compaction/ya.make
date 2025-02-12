PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/ya.make.inc)

TEST_SRCS(
    test.py
)

DATA(
    arcadia/cloud/blockstore/tests/loadtest/compaction
)

END()
