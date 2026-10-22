UNITTEST_FOR(contrib/ydb/core/kqp)

SIZE(MEDIUM)

SRCS(
    view_ut.cpp
)

PEERDIR(
    contrib/ydb/public/lib/ydb_cli/dump/util
    contrib/ydb/core/kqp/ut/common
    yql/essentials/sql
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1
    yql/essentials/utils/log

    contrib/ydb/core/testlib/basics
)

DATA(arcadia/contrib/ydb/core/kqp/ut/view/input)

YQL_LAST_ABI_VERSION()

END()
