UNITTEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCDIR(cloud/filestore/libs/vfs_fuse)

SRCS(
    fs_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/endpoint_vhost
    cloud/filestore/libs/vfs_fuse/fuse_virtio_client
    cloud/filestore/libs/vfs_fuse/vhost
)

END()
