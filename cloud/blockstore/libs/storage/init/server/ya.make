LIBRARY()

SRCS(
    actorsystem.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/libs/spdk/iface
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/disk_agent
    cloud/blockstore/libs/storage/disk_agent/model
    cloud/blockstore/libs/storage/disk_registry
    cloud/blockstore/libs/storage/disk_registry_proxy
    cloud/blockstore/libs/storage/disk_registry_proxy/model
    cloud/blockstore/libs/storage/init/common
    cloud/blockstore/libs/storage/partition
    cloud/blockstore/libs/storage/partition2
    cloud/blockstore/libs/storage/service
    cloud/blockstore/libs/storage/stats_service
    cloud/blockstore/libs/storage/ss_proxy
    cloud/blockstore/libs/storage/volume_throttling_manager
    cloud/blockstore/libs/storage/undelivered
    cloud/blockstore/libs/storage/volume
    cloud/blockstore/libs/storage/volume_balancer
    cloud/blockstore/libs/storage/volume_proxy

    cloud/storage/core/libs/api
    cloud/storage/core/libs/auth
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/hive_proxy
    cloud/storage/core/libs/kikimr
    cloud/storage/core/libs/user_stats

    ydb/library/actors/core
    ydb/library/actors/util

    ydb/core/base
    ydb/core/load_test
    ydb/core/mind
    ydb/core/mon
    ydb/core/node_whiteboard
    ydb/core/protos
    ydb/core/tablet
)

YQL_LAST_ABI_VERSION()

END()
