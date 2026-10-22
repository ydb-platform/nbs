UNITTEST()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/discovery
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/public/lib/ydb_cli/dump/util

    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1
)

SRCS(
    kqp_discovery_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
