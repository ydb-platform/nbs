UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/services/ydb
)

SRCS(
    ut_ru_calculator.cpp
)

YQL_LAST_ABI_VERSION()

END()
