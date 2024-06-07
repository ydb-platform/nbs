LIBRARY()

SRCS(
    helpers.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/iam
    contrib/ydb/public/sdk/cpp/client/ydb_types/credentials
    contrib/ydb/library/yql/public/issue/protos
)

END()
