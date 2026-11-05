UNITTEST_FOR(contrib/ydb/core/fq/libs/result_formatter)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    result_formatter_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib
    contrib/ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
