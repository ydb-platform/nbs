LIBRARY()

SRCS(
    client.cpp
)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/ydb_common_client/impl
)

END()
