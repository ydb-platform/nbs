LIBRARY()

SRCS(
    actorsystem.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/disk_agent
    cloud/blockstore/libs/storage/disk_agent/model
    cloud/blockstore/libs/storage/disk_registry_proxy
    cloud/blockstore/libs/storage/disk_registry_proxy/model
    cloud/blockstore/libs/storage/init/common
    cloud/blockstore/libs/storage/stats_fetcher
    cloud/blockstore/libs/storage/undelivered

    cloud/storage/core/libs/api
    cloud/storage/core/libs/hive_proxy

    contrib/ydb/library/actors/core
)

YQL_LAST_ABI_VERSION()

END()
