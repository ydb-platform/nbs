LIBRARY()

SRCS(
    client.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/library/yql/public/issue
    contrib/ydb/public/sdk/cpp/src/client/types
)

GENERATE_ENUM_SERIALIZATION(client.h)

END()
