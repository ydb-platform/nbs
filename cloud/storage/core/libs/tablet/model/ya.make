LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

SRCS(
    channels.cpp
    commit.cpp
    partial_blob_id.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
