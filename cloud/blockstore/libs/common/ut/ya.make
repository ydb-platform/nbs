UNITTEST_FOR(cloud/blockstore/libs/common)

IF (WITH_VALGRIND)
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)
ENDIF()

SRCS(
    caching_allocator_ut.cpp
    block_buffer_ut.cpp
    block_checksum_ut.cpp
    block_data_ref_ut.cpp
    block_range_ut.cpp
    guarded_sglist_ut.cpp
    iovector_ut.cpp
    sglist_ut.cpp
)

END()
