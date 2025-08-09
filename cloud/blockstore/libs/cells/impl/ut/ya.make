UNITTEST_FOR(cloud/blockstore/libs/cells/impl)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    describe_volume_ut.cpp
    cell_ut.cpp
    cells_ut.cpp
    host_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/cells/iface
    cloud/blockstore/libs/cells/impl
    cloud/blockstore/libs/client
    cloud/blockstore/libs/client_rdma
)

END()
