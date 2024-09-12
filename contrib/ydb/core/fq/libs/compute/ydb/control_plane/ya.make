LIBRARY()

SRCS(
    cms_grpc_client_actor.cpp
    compute_database_control_plane_service.cpp
    compute_databases_cache.cpp
    database_monitoring.cpp
    monitoring_grpc_client_actor.cpp
    ydbcp_grpc_client_actor.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/protos
    contrib/ydb/library/grpc/actor_client
    contrib/ydb/core/fq/libs/compute/ydb/synchronization_service
    contrib/ydb/core/fq/libs/control_plane_storage/proto
    contrib/ydb/core/fq/libs/quota_manager/proto
    contrib/ydb/core/protos
    contrib/ydb/library/db_pool/protos
    contrib/ydb/library/yql/public/issue
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/lib/operation_id/protos
)

YQL_LAST_ABI_VERSION()

END()
