PY3_PROGRAM(yc-nbs-ci-fio-performance-test-suite)

PY_SRCS(
    __main__.py
)

PEERDIR(
    cloud/blockstore/tools/ci/fio_performance_test_suite/lib
)

END()

RECURSE(
    lib
)

RECURSE_FOR_TESTS(
    tests
)
