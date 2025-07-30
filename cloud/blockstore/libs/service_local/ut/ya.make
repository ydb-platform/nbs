UNITTEST_FOR(cloud/blockstore/libs/service_local)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    compound_storage_ut.cpp
    file_io_service_provider_ut.cpp
    safe_deallocator_ut.cpp
    storage_local_ut.cpp
    storage_null_ut.cpp
    storage_rdma_ut.cpp
    storage_spdk_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/server
    cloud/blockstore/libs/rdma/iface

    cloud/storage/core/libs/aio
)

END()
