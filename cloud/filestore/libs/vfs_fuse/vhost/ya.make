LIBRARY()

CFLAGS(
    -DFUSE_VIRTIO
    -DFUSE_USE_VERSION=31
)

SRCS(
    fuse_virtio.c
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/libs/vfs_fuse/ya.make.inc)

SRCDIR(cloud/filestore/libs/vfs_fuse)

PEERDIR(
    cloud/contrib/vhost
    contrib/libs/virtiofsd
)

END()
