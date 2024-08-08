LIBRARY()

SRCS(
    synchronization_service.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/protos
    contrib/ydb/library/grpc/actor_client
    contrib/ydb/core/fq/libs/control_plane_storage/proto
    contrib/ydb/public/api/grpc
    contrib/ydb/library/db_pool/protos
    contrib/ydb/public/lib/operation_id/protos
    contrib/ydb/core/fq/libs/quota_manager/proto
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/library/services
)

YQL_LAST_ABI_VERSION()

END()
