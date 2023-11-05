UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    # kqp_executer_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/host
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
