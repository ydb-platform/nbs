PY3_PROGRAM(yc-nbs-ci-check-nrd-disk-emptiness-test)

PY_SRCS(
    __main__.py
)

PEERDIR(
    cloud/blockstore/pylibs/common
    cloud/blockstore/tools/ci/check_emptiness_test/lib
)

END()

RECURSE(lib)

RECURSE_FOR_TESTS(
    tests
)
