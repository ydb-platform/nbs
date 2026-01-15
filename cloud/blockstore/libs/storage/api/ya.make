LIBRARY()

SRCS(
    bootstrapper.cpp
    disk_agent.cpp
    disk_registry.cpp
    disk_registry_proxy.cpp
    partition.cpp
    partition2.cpp
    service.cpp
    ss_proxy.cpp
    stats_service.cpp
    volume_throttling_manager.cpp
    undelivered.cpp
    volume.cpp
    volume_balancer.cpp
    volume_proxy.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/service
    cloud/blockstore/libs/storage/protos
    cloud/blockstore/libs/storage/protos_ydb
    cloud/blockstore/private/api/protos
    cloud/blockstore/public/api/protos
    contrib/ydb/library/actors/core
    contrib/ydb/core/protos
)

END()
