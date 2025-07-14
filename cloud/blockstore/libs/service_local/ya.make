LIBRARY()

SRCS(
    broken_storage.cpp
    compound_storage.cpp
    file_io_service_provider.cpp
    rdma_protocol.cpp
    service_local.cpp
    storage_local.cpp
    storage_null.cpp
    storage_rdma.cpp
    storage_spdk.cpp
    zero_extent.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/nvme
    cloud/blockstore/libs/service
    cloud/blockstore/libs/spdk/iface
    cloud/blockstore/libs/storage/protos

    cloud/storage/core/libs/common

    library/cpp/aio
    library/cpp/deprecated/atomic
    library/cpp/protobuf/util

    contrib/libs/protobuf
)

END()

RECURSE_FOR_TESTS(
    ut
)

RECURSE_FOR_TESTS(
    ut_large
)
