UNITTEST_FOR(contrib/ydb/apps/etcd_proxy/service)

SIZE(MEDIUM)

SRCS(
    etcd_service_ut.cpp
)

PEERDIR(
    library/cpp/logger
    contrib/ydb/core/protos
    contrib/ydb/core/testlib/default
    contrib/ydb/apps/etcd_proxy/service
    contrib/ydb/services/keyvalue
)

YQL_LAST_ABI_VERSION()

END()
