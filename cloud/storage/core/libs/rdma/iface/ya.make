LIBRARY()

SRCS(
    buffer.cpp
    client.cpp
    config.cpp
    log.cpp
    probes.cpp
    protobuf.cpp
    protocol.cpp
    proxy.cpp
    server.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/protos

    library/cpp/threading/future

    contrib/libs/protobuf
)

END()

RECURSE_FOR_TESTS(
    ut
)
