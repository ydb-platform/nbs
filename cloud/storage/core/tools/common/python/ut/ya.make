PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

TEST_SRCS(
    core_pattern_ut.py
)

PEERDIR(
    cloud/storage/core/tools/common/python
)

END()
