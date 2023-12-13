UNITTEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCDIR(cloud/filestore/libs/vfs_fuse)

SRCS(
    fs_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/vfs_fuse/vhost
    cloud/filestore/libs/vhost
)

END()
