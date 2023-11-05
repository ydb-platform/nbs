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
    library/cpp/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/mon
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/basics
)

END()

RECURSE(
    model
)

RECURSE_FOR_TESTS(
    ut
)
