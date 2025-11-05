LIBRARY()

SRCS(
    discovery.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_common_client/impl
    contrib/ydb/public/sdk/cpp/client/ydb_driver
)

END()
