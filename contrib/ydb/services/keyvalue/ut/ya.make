UNITTEST_FOR(contrib/ydb/services/keyvalue)

SIZE(MEDIUM)

SRCS(
    grpc_service_ut.cpp
)

PEERDIR(
    library/cpp/logger
    contrib/ydb/core/protos
    contrib/ydb/core/testlib/default
    contrib/ydb/services/keyvalue
)

YQL_LAST_ABI_VERSION()

END()
