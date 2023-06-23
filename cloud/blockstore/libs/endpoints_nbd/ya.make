LIBRARY()

SRCS(
    nbd_server.cpp
)

PEERDIR(
    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/endpoints
    cloud/blockstore/libs/nbd
    cloud/blockstore/libs/service
)

END()
