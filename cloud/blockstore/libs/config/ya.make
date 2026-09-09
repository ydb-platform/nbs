# The shared BlockStore configuration, holder, and process provider library.
# Runtime consumers link this module to read one consistent snapshot.

LIBRARY()

SRCS(
    blockstore_config.cpp
    blockstore_config_holder.cpp
    blockstore_config_provider.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/cells/iface
    cloud/blockstore/libs/client
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/discovery
    cloud/blockstore/libs/local_nvme
    cloud/blockstore/libs/logbroker/iface
    cloud/blockstore/libs/notify/iface
    cloud/blockstore/libs/rdma
    cloud/blockstore/libs/server
    cloud/blockstore/libs/spdk/iface
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/disk_agent/model
    cloud/blockstore/libs/storage/disk_registry_proxy/model
    cloud/blockstore/libs/ydbstats
    cloud/storage/core/libs/features
    cloud/storage/core/libs/iam/iface

    library/cpp/threading/hot_swap
)

END()

RECURSE_FOR_TESTS(
    ut
)
