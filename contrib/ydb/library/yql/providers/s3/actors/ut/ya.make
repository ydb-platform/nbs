IF (NOT OS_WINDOWS)

UNITTEST_FOR(contrib/ydb/library/yql/providers/s3/actors)

TAG(ya:manual)

SRCS(
    yql_arrow_push_down_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

ENDIF()

