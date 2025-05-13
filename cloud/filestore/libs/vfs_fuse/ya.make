LIBRARY()

CFLAGS(
    -DFUSE_USE_VERSION=29
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/libs/vfs_fuse/ya.make.inc)

SRCDIR(cloud/filestore/libs/vfs_fuse)

SRCS(
    fuse.cpp
    vhost/fuse_virtio.c
)

PEERDIR(
    contrib/libs/fuse
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(
        tsan.supp
    )
ENDIF()

END()

RECURSE(
    protos
    vhost
)

RECURSE_FOR_TESTS(
    fuzz
    ut
)
