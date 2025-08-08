LIBRARY()

SRCS(
    cell_host.cpp
    cell.cpp
    cells.cpp
    describe_volume.cpp
    endpoints_setup.cpp
    remote_storage.cpp
)

PEERDIR(
    cloud/blockstore/libs/cells/iface
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/rdma/impl
    cloud/blockstore/libs/service
)

END()

RECURSE_FOR_TESTS(ut)
