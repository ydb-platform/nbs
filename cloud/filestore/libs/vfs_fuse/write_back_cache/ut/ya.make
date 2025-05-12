UNITTEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

SRCDIR(cloud/filestore/libs/vfs_fuse/write_back_cache)

SRCS(
    calculate_data_parts_to_read_ut.cpp
    session_sequencer_ut.cpp
    write_back_cache_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/vfs_fuse/write_back_cache
)

END()
