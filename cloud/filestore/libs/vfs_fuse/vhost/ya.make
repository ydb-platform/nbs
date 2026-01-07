LIBRARY()

CFLAGS(
    -DFUSE_VIRTIO
    -DFUSE_USE_VERSION=31
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/libs/vfs_fuse/ya.make.inc)

SRCDIR(cloud/filestore/libs/vfs_fuse)

SRCS(
    # virtio-fs glue for FUSE
    vhost/fuse_virtio.c
)

PEERDIR(
    cloud/contrib/vhost
    cloud/contrib/virtiofsd
)

END()
