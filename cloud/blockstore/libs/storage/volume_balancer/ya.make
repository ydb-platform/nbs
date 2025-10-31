LIBRARY()

GENERATE_ENUM_SERIALIZATION(volume_balancer_state.h)

SRCS(
    volume_balancer.cpp
    volume_balancer_actor.cpp
    volume_balancer_actor_monitoring.cpp
    volume_balancer_state.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/protos
    cloud/blockstore/private/api/protos
    ydb/library/actors/core
    library/cpp/monlib/service/pages
    ydb/core/base
    ydb/core/blockstore/core
    ydb/core/cms/console
    ydb/core/mind
    ydb/core/mon
    ydb/core/node_whiteboard
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat
)

END()

RECURSE_FOR_TESTS(
    ut
)
