LIBRARY()

SRCS(
    bootstrap.cpp
    config_initializer.cpp
    options.cpp
)

PEERDIR(
    cloud/blockstore/libs/cells/impl
    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/daemon/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/discovery
    cloud/blockstore/libs/endpoints
    cloud/blockstore/libs/kms/iface
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/logbroker/iface
    cloud/blockstore/libs/notify
    cloud/blockstore/libs/nvme
    cloud/blockstore/libs/rdma/fake
    cloud/blockstore/libs/root_kms/iface
    cloud/blockstore/libs/server
    cloud/blockstore/libs/service
    cloud/blockstore/libs/service_kikimr
    cloud/blockstore/libs/service_local
    cloud/blockstore/libs/spdk/iface
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/disk_agent
    cloud/blockstore/libs/storage/disk_registry_proxy
    cloud/blockstore/libs/storage/init/server
    cloud/blockstore/libs/ydbstats

    cloud/storage/core/config
    cloud/storage/core/libs/actors
    cloud/storage/core/libs/aio
    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/iam/iface
    cloud/storage/core/libs/kikimr
    cloud/storage/core/libs/version

    library/cpp/json
    library/cpp/lwtrace/mon
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/service/pages
    library/cpp/protobuf/util

    contrib/ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
