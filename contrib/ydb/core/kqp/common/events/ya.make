LIBRARY()

SRCS(
    events.cpp
    query.cpp
    workload_service.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/grpc_services/cancelation
    contrib/ydb/core/kqp/common/shutdown
    contrib/ydb/core/kqp/common/compilation
    contrib/ydb/core/resource_pools
    contrib/ydb/core/scheme

    contrib/ydb/library/yql/dq/actors
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/library/operation_id

    contrib/ydb/library/actors/core
)

GENERATE_ENUM_SERIALIZATION(script_executions.h)

YQL_LAST_ABI_VERSION()

END()
