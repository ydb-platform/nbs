UNITTEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

SRCDIR(cloud/filestore/libs/vfs_fuse/counters)

SRCS(
    relaxed_counters_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/vfs_fuse/counters
)

END()
