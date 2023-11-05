UNITTEST_FOR(contrib/ydb/core/tx/scheme_board)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

SRCS(
    double_indexed_ut.cpp
)

END()
