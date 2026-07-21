UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()

SPLIT_FACTOR(5)
SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    kqp_arrow_in_channels_ut.cpp
    kqp_types_arrow_ut.cpp
    kqp_result_set_formats_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/public/sdk/cpp/src/client/arrow
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()
