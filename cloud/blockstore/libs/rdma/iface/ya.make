LIBRARY()

SRCS(
    client.cpp
    config.cpp
    probes.cpp
    protobuf.cpp
    protocol.cpp
    server.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    library/cpp/threading/future

    contrib/libs/protobuf
)

END()

RECURSE_FOR_TESTS(
    ut
)
