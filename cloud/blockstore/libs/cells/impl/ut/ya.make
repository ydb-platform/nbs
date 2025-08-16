UNITTEST_FOR(cloud/blockstore/libs/cells/impl)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    cell_host_impl_ut.cpp
    cell_impl_ut.cpp
    describe_volume_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/cells/iface
    cloud/blockstore/libs/cells/impl
    cloud/blockstore/libs/client
    cloud/blockstore/libs/client_rdma
)

END()
