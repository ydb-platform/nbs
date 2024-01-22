LIBRARY()

SRCS(
    migration_timeout_calculator.cpp
    processing_blocks.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/storage/core
    cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(ut)
