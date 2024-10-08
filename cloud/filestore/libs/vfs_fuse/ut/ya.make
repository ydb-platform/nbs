UNITTEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

SRCDIR(cloud/filestore/libs/vfs_fuse)

SRCS(
    file_ring_buffer_ut.cpp
    fs_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/vfs_fuse/vhost
    cloud/filestore/libs/vhost
)

END()
