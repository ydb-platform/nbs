UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/testlib/pg
    contrib/ydb/core/tx
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/public/api/protos
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_base.cpp
    ut_counters.cpp
    ut_info_types.cpp
    ut_move_tablet_to_storage_pool.cpp
    ut_table_decimal_types.cpp
    ut_table_info.cpp
    ut_table_pg_types.cpp
    ut_commit_redo_limit.cpp
)

END()
