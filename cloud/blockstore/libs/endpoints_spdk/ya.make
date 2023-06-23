LIBRARY()

SRCS(
    spdk_server.cpp
)

PEERDIR(
    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/endpoints
    cloud/blockstore/libs/service
    cloud/blockstore/libs/spdk/iface

    cloud/storage/core/libs/coroutine
)

END()
