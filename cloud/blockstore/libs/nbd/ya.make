LIBRARY()

SRCS(
    binary_reader.cpp
    binary_writer.cpp
    client.cpp
    client_handler.cpp
    limiter.cpp
    protocol.cpp
    server.cpp
    server_handler.cpp
    utils.cpp
)

IF (OS_LINUX)
    SRCS(
        device.cpp
    )

IF (NETLINK)
    SRCS(
        netlink_device.cpp
    )
    CFLAGS(
        -I/usr/include/libnl3
    )
    LDFLAGS(
        -L/usr/lib/x86_64-linux-gnu
        -lnl-3
        -lnl-genl-3
    )
ENDIF()

PEERDIR(
    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service
    cloud/storage/core/libs/coroutine
    contrib/libs/linux-headers
    library/cpp/coroutine/engine
    library/cpp/coroutine/listener
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
