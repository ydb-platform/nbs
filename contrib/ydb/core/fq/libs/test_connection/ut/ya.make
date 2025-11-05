UNITTEST_FOR(contrib/ydb/core/fq/libs/control_plane_proxy)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    test_connection_ut.cpp
)

END()
