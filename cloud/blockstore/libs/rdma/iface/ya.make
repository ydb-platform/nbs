LIBRARY()

SRCS(
    client.cpp
    error.cpp
    list.cpp
    poll.cpp
    probes.cpp
    protobuf.cpp
    protocol.cpp
    rcu.cpp
    server.cpp
    utils.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    library/cpp/monlib/dynamic_counters
    library/cpp/threading/future
    library/cpp/deprecated/atomic

    contrib/libs/protobuf
)

END()

RECURSE_FOR_TESTS(
    ut
)
