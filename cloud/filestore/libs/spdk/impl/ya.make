LIBRARY()

SRCS(
    alloc.cpp
    env.cpp
)

ADDINCL(
    cloud/filestore/libs/spdk/lib/include
)

PEERDIR(
    #cloud/blockstore/libs/common
    #cloud/blockstore/libs/diagnostics
    cloud/filestore/libs/spdk/iface

    #cloud/storage/core/libs/common
    #cloud/storage/core/libs/diagnostics
)

END()
