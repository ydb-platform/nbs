LIBRARY()

SRCS(
    buffer.cpp
    client.cpp
    config.cpp
    probes.cpp
    protobuf.cpp
    protocol.cpp
    server.cpp
    log.cpp
)

PEERDIR(
    cloud/storage/config
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    library/cpp/threading/future

    contrib/libs/protobuf
)

END()

RECURSE_FOR_TESTS(
    ut
)
