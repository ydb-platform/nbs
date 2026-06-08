LIBRARY()

SRCS(
    dynamic_persistent_table.cpp
    dynamic_persistent_table_counters.cpp
    file_map_memory_limiter.cpp
    file_ring_buffer.cpp
    persistent_table.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
)

END()

RECURSE_FOR_TESTS(
    ut
)
