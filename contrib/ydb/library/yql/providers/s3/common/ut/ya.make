UNITTEST_FOR(contrib/ydb/library/yql/providers/s3/common)

SRCS(
    util_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
)

END()
