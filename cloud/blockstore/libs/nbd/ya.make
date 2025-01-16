LIBRARY()

SRCS(
    binary_reader.cpp
    binary_writer.cpp
    client.cpp
    client_handler.cpp
    error_handler.cpp
    limiter.cpp
    protocol.cpp
    server.cpp
    server_handler.cpp
    utils.cpp
)

IF (OS_LINUX)
    SRCS(
        device.cpp
        netlink_device.cpp
    )
ENDIF()

PEERDIR(
    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/netlink
    contrib/libs/linux-headers
    library/cpp/coroutine/engine
    library/cpp/coroutine/listener
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
