UNITTEST()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/discovery
    contrib/ydb/core/kqp/ut/common

    yql/essentials/sql/pg_dummy
)

SRCS(
    kqp_discovery_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
