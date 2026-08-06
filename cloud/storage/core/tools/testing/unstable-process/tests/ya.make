PY3TEST()

TEST_SRCS(test.py)

DEPENDS(
    cloud/storage/core/tools/testing/unstable-process
    cloud/storage/core/tools/testing/unstable-process/tests/dummy-daemon
)

PEERDIR(
    cloud/storage/core/tools/common/python

    library/python/filelock
    library/python/testing/yatest_common
)

SIZE(MEDIUM)

END()

RECURSE(
    dummy-daemon
)
