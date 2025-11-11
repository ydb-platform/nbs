LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

SRCS(
    executor.cpp
    queue.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    library/cpp/coroutine/engine
    library/cpp/threading/future
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
