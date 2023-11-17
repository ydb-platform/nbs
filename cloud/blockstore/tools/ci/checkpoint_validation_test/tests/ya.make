PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/tools/ci/checkpoint_validation_test
)

END()
