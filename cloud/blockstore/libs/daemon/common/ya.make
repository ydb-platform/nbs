LIBRARY()

GENERATE_ENUM_SERIALIZATION(options.h)

SRCS(
    bootstrap.cpp
    config_initializer.cpp
    options.cpp
)

PEERDIR(
    cloud/blockstore/libs/cells/iface
    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/discovery
    cloud/blockstore/libs/endpoint_proxy/client
    cloud/blockstore/libs/endpoints
    cloud/blockstore/libs/endpoints_grpc
    cloud/blockstore/libs/endpoints_nbd
    cloud/blockstore/libs/endpoints_rdma
    cloud/blockstore/libs/endpoints_spdk
    cloud/blockstore/libs/endpoints_vhost
    cloud/blockstore/libs/encryption
    cloud/blockstore/libs/nbd
    cloud/blockstore/libs/nvme
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/libs/server
    cloud/blockstore/libs/service
    cloud/blockstore/libs/service_local
    cloud/blockstore/libs/service_rdma
    cloud/blockstore/libs/service_throttling
    cloud/blockstore/libs/spdk/iface
    cloud/blockstore/libs/storage/disk_agent/model
    cloud/blockstore/libs/storage/disk_registry_proxy/model
    cloud/blockstore/libs/throttling
    cloud/blockstore/libs/validation
    cloud/blockstore/libs/vhost

    cloud/storage/core/libs/aio
    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/daemon
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/endpoints/fs
    cloud/storage/core/libs/endpoints/keyring
    cloud/storage/core/libs/opentelemetry/iface
    cloud/storage/core/libs/opentelemetry/impl
    cloud/storage/core/libs/version

    contrib/ydb/library/actors/util
    library/cpp/getopt/small
    library/cpp/json
    library/cpp/json/writer
    library/cpp/logger
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    library/cpp/monlib/dynamic_counters
    library/cpp/protobuf/util
    library/cpp/sighandler
)

END()
