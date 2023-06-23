UNITTEST_FOR(cloud/blockstore/libs/common)

IF (WITH_VALGRIND)
    TIMEOUT(600)
    SIZE(MEDIUM)
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
