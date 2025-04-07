PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/ya.make.inc)

REQUIREMENTS(
    cpu:8
    ram:40
) 

TEST_SRCS(
    test.py
)

DATA(
    arcadia/cloud/blockstore/tests/loadtest/local-encryption
)

END()
