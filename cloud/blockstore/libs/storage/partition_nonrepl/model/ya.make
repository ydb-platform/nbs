LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/deny_ydb_dependency.inc)

SRCS(
    processing_blocks.cpp
)

PEERDIR(
    cloud/blockstore/libs/common

    cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(ut)
