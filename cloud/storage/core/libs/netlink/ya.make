LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

SRCS(
    message.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
)

END()
