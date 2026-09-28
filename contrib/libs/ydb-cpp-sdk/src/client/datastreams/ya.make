LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    datastreams.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/library/grpc/client
    library/cpp/string_utils/url
    contrib/ydb/public/api/grpc/draft
    contrib/libs/ydb-cpp-sdk/src/library/operation_id
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_internal/make_request
    contrib/libs/ydb-cpp-sdk/src/client/driver
)

END()
