UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    SPLIT_FACTOR(40)
ENDIF()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/testlib/pg
    contrib/ydb/core/tx
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/public/api/protos
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_base.cpp
    ut_counters.cpp
    ut_info_types.cpp
    ut_table_decimal_types.cpp
    ut_table_pg_types.cpp
    ut_commit_redo_limit.cpp
)

END()
