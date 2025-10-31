LIBRARY()

SRCS(
    disk_registry_proxy.cpp
    disk_registry_proxy_actor.cpp
    disk_registry_proxy_actor_create.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/disk_registry_proxy/model
    cloud/storage/core/libs/api
    ydb/library/actors/core
    ydb/core/base
    ydb/core/mon
    ydb/core/tablet
    ydb/core/tablet_flat
)

END()

RECURSE(
    model
)

RECURSE_FOR_TESTS(
    ut
)
