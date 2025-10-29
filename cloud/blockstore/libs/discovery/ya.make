LIBRARY()

SRCS(
    balancing.cpp
    ban.cpp
    config.cpp
    discovery.cpp
    fetch.cpp
    healthcheck.cpp
    instance.cpp
    ping.cpp
    test_conductor.cpp
    test_server.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/public/api/grpc
    cloud/blockstore/public/api/protos

    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/storage/core/libs/grpc

    ydb/library/actors/prof
    library/cpp/deprecated/atomic
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/service/pages    # for config html dump
    library/cpp/neh
    library/cpp/threading/future

    contrib/libs/grpc
)

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
ENDIF()

END()

RECURSE_FOR_TESTS(ut)
