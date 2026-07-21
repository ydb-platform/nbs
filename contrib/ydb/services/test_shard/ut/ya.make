UNITTEST_FOR(contrib/ydb/services/test_shard)

SIZE(MEDIUM)

SRCS(
    grpc_service_ut.cpp
)

PEERDIR(
    library/cpp/logger
    contrib/ydb/core/protos
    contrib/ydb/core/testlib/default
    contrib/ydb/services/test_shard
    contrib/ydb/core/test_tablet
)

TIMEOUT(60)

YQL_LAST_ABI_VERSION()

END()
