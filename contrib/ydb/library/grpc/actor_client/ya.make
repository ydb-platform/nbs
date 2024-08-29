LIBRARY()

SRCS(
    grpc_service_cache.h
    grpc_service_client.h
    grpc_service_settings.h
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/util
    library/cpp/digest/crc32c
    contrib/ydb/library/grpc/client
    contrib/ydb/library/services
)

END()
