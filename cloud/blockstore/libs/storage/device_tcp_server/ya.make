LIBRARY()

SRCS(
    server.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/device_tcp_server/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics
)

END()

RECURSE_FOR_TESTS(
    ut
)
