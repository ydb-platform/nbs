UNITTEST_FOR(cloud/storage/core/libs/file_backed_containers)

# Randomized ring-buffer tests exercise both V5 and V6 and can exceed
# the small-test chunk timeout even without sanitizers.
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCDIR(cloud/storage/core/libs/file_backed_containers)

SRCS(
    dynamic_persistent_table_crash_ut.cpp
    dynamic_persistent_table_ut.cpp
    file_map_memory_limiter_ut.cpp
    file_ring_buffer_ut.cpp
    file_ring_buffer_accessor_ut.cpp
    file_ring_buffer_format_ut.cpp
    persistent_table_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/file_backed_containers
)

END()
