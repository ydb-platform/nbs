UNITTEST_FOR(contrib/ydb/core/kqp)

SIZE(MEDIUM)

SRCS(
    view_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/ut/common
    yql/essentials/sql
    yql/essentials/sql/v1
    yql/essentials/utils/log

    contrib/ydb/core/testlib/basics/default
)

DATA(arcadia/contrib/ydb/core/kqp/ut/view/input)

YQL_LAST_ABI_VERSION()

END()
