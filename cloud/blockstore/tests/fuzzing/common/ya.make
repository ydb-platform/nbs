LIBRARY()

SRCS(
    starter.cpp
)

ADDINCL(
    cloud/contrib/vhost
)

PEERDIR(
    cloud/blockstore/libs/daemon/common
    cloud/blockstore/libs/service

    cloud/storage/core/libs/common
    cloud/storage/core/libs/daemon
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/vhost-client
)

END()
