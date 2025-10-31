LIBRARY()

SRCS(
    stats_service.cpp
    stats_service_actor.cpp
    stats_service_actor_bandwidth_limiter.cpp
    stats_service_actor_client_stats.cpp
    stats_service_actor_solomon.cpp
    stats_service_actor_ydb.cpp
    stats_service_state.cpp
    stats_service_volume_stats.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/discovery
    cloud/blockstore/libs/encryption
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/protos
    cloud/blockstore/libs/storage/volume
    cloud/blockstore/libs/ydbstats
    cloud/blockstore/private/api/protos

    cloud/storage/core/libs/api

    ydb/core/base
    ydb/core/mind
    ydb/core/mon
    ydb/core/protos
    ydb/core/tablet

    ydb/library/actors/core
    library/cpp/json
    library/cpp/monlib/service/pages

    contrib/libs/protobuf
)

END()

RECURSE_FOR_TESTS(ut)
