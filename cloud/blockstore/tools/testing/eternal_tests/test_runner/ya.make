PY3_PROGRAM(yc-nbs-run-eternal-load-tests)

PY_SRCS(
    __main__.py
)

PEERDIR(
    cloud/blockstore/pylibs/clusters/test_config
    cloud/blockstore/pylibs/common
    cloud/blockstore/pylibs/ycp
    cloud/blockstore/tools/testing/eternal_tests/test_runner/lib
)

RESOURCE(
    configs/config.json test-config.json
    scripts/mysql.sh mysql.sh
    scripts/oltp-custom.lua oltp-custom.lua
    scripts/postgresql.sh postgresql.sh
    scripts/mysql-nfs.sh mysql-nfs.sh
    scripts/postgresql-nfs.sh postgresql-nfs.sh
)

END()

RECURSE(
    lib
)

RECURSE_FOR_TESTS(
    tests
)
