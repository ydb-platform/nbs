UNITTEST_FOR(cloud/blockstore/libs/client_rdma)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    rdma_client_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/service

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/rdma/iface
)

END()
