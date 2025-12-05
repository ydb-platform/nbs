UNITTEST_FOR(cloud/blockstore/libs/common)

IF (WITH_VALGRIND)
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)
ENDIF()

SRCS(
    block_checksum_ut.cpp
    block_range_list_ut.cpp
    block_range_ut.cpp
    caching_allocator_ut.cpp
    cgroups_helpers_ut.cpp
    device_path_ut.cpp
    iovector_ut.cpp
)

PEERDIR(
    library/cpp/uri
)

END()
