LIBRARY()

SRCS(
    buffer.cpp
    client.cpp
    server.cpp
    test_verbs.cpp
    verbs.cpp
    work_queue.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/libs/service
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    library/cpp/monlib/dynamic_counters
    library/cpp/threading/future
    contrib/libs/ibdrv
    contrib/libs/protobuf
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)

