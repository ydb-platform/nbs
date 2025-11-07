LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

SRCS(
    context.cpp
    service.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    contrib/libs/liburing
)

END()

RECURSE_FOR_TESTS(ut)
