LIBRARY()

SRCS(
    health.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/fq/libs/shared_resources
    contrib/ydb/core/mon
    contrib/ydb/public/sdk/cpp/client/ydb_discovery
)

YQL_LAST_ABI_VERSION()

END()
