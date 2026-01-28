UNITTEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

SRCDIR(cloud/filestore/libs/vfs_fuse/write_back_cache)

SRCS(
    disjoint_interval_builder_ut.cpp
    node_cache_ut.cpp
    overlapping_interval_set_ut.cpp
    persistent_request_storage_ut.cpp
    persistent_storage_ut.cpp
    read_write_range_lock_ut.cpp
    write_back_cache_state_ut.cpp
    write_back_cache_ut.cpp
    write_back_cache_util_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/vfs_fuse/write_back_cache
)

END()
