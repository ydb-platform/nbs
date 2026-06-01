UNITTEST_FOR(cloud/storage/core/libs/file_backed_containers)

# TFileRingBufferTest::RandomizedPushPopRestore tests may need more time
# under sanitizers
IF (SANITIZER_TYPE OR WITH_VALGRIND)
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)
ENDIF()

SRCDIR(cloud/storage/core/libs/file_backed_containers)

SRCS(
    dynamic_persistent_table_crash_ut.cpp
    dynamic_persistent_table_ut.cpp
    file_map_memory_limiter_ut.cpp
    file_ring_buffer_ut.cpp
    persistent_table_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/file_backed_containers
)

END()
