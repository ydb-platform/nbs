LIBRARY()

LICENSE_RESTRICTION_EXCEPTIONS(
    contrib/restricted/libnl/lib/nl-3
    contrib/restricted/libnl/lib/nl-genl-3
)

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
        netlink_device.cpp
    )
ENDIF()

PEERDIR(
    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service
    cloud/storage/core/libs/coroutine
    contrib/libs/linux-headers
    contrib/restricted/libnl/lib/nl-3
    contrib/restricted/libnl/lib/nl-genl-3
    library/cpp/coroutine/engine
    library/cpp/coroutine/listener
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
