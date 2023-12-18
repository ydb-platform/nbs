PY3_PROGRAM(yc-nfs-ci-xfs-test-suite)

PY_SRCS(
    __main__.py
)

PEERDIR(
    cloud/blockstore/pylibs/common

    cloud/filestore/tests/xfs_suite/lib
)

END()

RECURSE_FOR_TESTS(
    tests
)
