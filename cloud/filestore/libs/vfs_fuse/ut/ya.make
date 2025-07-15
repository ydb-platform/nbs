UNITTEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

SRCDIR(cloud/filestore/libs/vfs_fuse)

SRCS(
    fs_ut.cpp
    fs_impl_ut.cpp
)

PEERDIR(
    cloud/blockstore/public/api/protos
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/vfs_fuse/vhost
    cloud/filestore/libs/vhost
)

END()
