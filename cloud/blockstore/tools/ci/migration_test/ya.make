PY3_PROGRAM(yc-nbs-ci-migration-test)

PY_SRCS(
    __main__.py
)

PEERDIR(
    cloud/blockstore/pylibs/common
    cloud/blockstore/tools/ci/migration_test/lib
)

END()

RECURSE(lib)

RECURSE_FOR_TESTS(
    tests
)
