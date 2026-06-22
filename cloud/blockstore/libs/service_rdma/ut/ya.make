UNITTEST_FOR(cloud/blockstore/libs/service_rdma)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    rdma_target_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/service
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/rdma/iface
)

END()
