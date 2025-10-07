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
    contrib/ydb/library/actors/core
    library/cpp/monlib/service/pages
    contrib/ydb/core/base
    contrib/ydb/core/blockstore/core
    contrib/ydb/core/cms/console
    contrib/ydb/core/mind
    contrib/ydb/core/mon
    contrib/ydb/core/node_whiteboard
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
)

END()

RECURSE_FOR_TESTS(
    ut
)
