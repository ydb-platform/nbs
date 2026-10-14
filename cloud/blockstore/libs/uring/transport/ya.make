LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

SRCS(
    config.cpp
    connection.cpp
    ring.cpp
    socket.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/protos
    contrib/libs/liburing
    library/cpp/monlib/dynamic_counters
    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(ut)
