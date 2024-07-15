PY3TEST()

DATA(
    arcadia/contrib/ydb/tests/functional/postgresql/cases
    sbr://4966407557=psql
)

DEPENDS(
    contrib/ydb/apps/ydbd
    contrib/ydb/apps/pgwire
)

ENV(PYTHONWARNINGS="ignore")
ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
SIZE(MEDIUM)

TEST_SRCS(
    test_postgres.py
)

ENV(PGWIRE_BINARY="contrib/ydb/apps/pgwire/pgwire")

PEERDIR(
    library/python/testing/yatest_common
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/ydb/tests/functional/postgresql/common
)

END()
