UNITTEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

SRCDIR(cloud/filestore/libs/vfs_fuse/write_back_cache)

SRCS(
    disjoint_interval_builder_ut.cpp
    overlapping_interval_set_ut.cpp
    persistent_storage_ut.cpp
    read_write_range_lock_ut.cpp
    test/test_persistent_storage.cpp
    test/test_write_back_cache_stats.cpp
    write_back_cache_ut.cpp
    write_back_cache_util_ut.cpp
    write_data_request_builder_ut.cpp
    write_data_request_manager_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/vfs_fuse/write_back_cache
)

END()
