LIBRARY()

SRCS(
    cell_proxy.cpp
)

PEERDIR(
    cloud/blockstore/libs/cells/iface
    cloud/blockstore/libs/client
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/storage/core/libs/actors
    cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
