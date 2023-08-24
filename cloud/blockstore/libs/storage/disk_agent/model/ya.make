LIBRARY()

SRCS(
    config.cpp
    device_client.cpp
    device_guard.cpp
    probes.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/service
    cloud/blockstore/public/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/protos
)

END()

RECURSE_FOR_TESTS(ut)
