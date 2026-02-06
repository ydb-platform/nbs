UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()

SPLIT_FACTOR(5)
SIZE(MEDIUM)

SRCS(
    kqp_arrow_in_channels_ut.cpp
    kqp_types_arrow_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
