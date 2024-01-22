UNITTEST_FOR(cloud/blockstore/libs/storage/partition_nonrepl/model)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    migration_timeout_calculator_ut.cpp
    processing_blocks_ut.cpp
)

END()
