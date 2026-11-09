UNITTEST_FOR(contrib/ydb/core/tx/mediator)

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/core/tx/coordinator/public
    contrib/ydb/core/tx/time_cast
    contrib/ydb/public/api/grpc
    yql/essentials/sql/v1_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    mediator_ut.cpp
)

END()
