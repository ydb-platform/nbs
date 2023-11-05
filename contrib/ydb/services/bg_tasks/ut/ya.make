UNITTEST_FOR(contrib/ydb/services/bg_tasks)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/testlib
    contrib/ydb/services/bg_tasks
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/services/bg_tasks/abstract
    contrib/ydb/services/bg_tasks
    library/cpp/testing/unittest
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/sql/pg
)

SRCS(
    ut_tasks.cpp
    GLOBAL task_emulator.cpp
)

SIZE(MEDIUM)
YQL_LAST_ABI_VERSION()

END()
