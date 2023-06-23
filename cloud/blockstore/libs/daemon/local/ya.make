LIBRARY()

SRCS(
    bootstrap.cpp
    config_initializer.cpp
    options.cpp
)

PEERDIR(
    cloud/blockstore/libs/daemon/common
    cloud/blockstore/libs/diagnostics
)

END()
