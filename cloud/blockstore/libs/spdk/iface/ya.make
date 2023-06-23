LIBRARY()

SRCS(
    config.cpp
    device.cpp
    env.cpp
    env_stub.cpp
    env_test.cpp
    target.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics

    cloud/storage/core/libs/diagnostics
)

END()
