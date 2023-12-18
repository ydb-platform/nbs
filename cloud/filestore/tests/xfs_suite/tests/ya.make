PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/tests/xfs_suite
)

END()
