UNITTEST_FOR(cloud/storage/core/libs/file_backed_containers)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCDIR(cloud/storage/core/libs/file_backed_containers)

SRCS(
    dynamic_persistent_table_ut.cpp
    file_map_memory_limiter_ut.cpp
    file_ring_buffer_ut.cpp
    persistent_table_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/file_backed_containers
)

END()
