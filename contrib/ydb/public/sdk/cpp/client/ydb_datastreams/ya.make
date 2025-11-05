LIBRARY()

SRCS(
    datastreams.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/client
    library/cpp/string_utils/url
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/lib/operation_id
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/make_request
    contrib/ydb/public/sdk/cpp/client/ydb_driver
)

END()
