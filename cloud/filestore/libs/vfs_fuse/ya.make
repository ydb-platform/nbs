LIBRARY()

CFLAGS(
    -DFUSE_USE_VERSION=29
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/libs/vfs_fuse/ya.make.inc)

SRCS(
    fuse.cpp
)

PEERDIR(
    contrib/libs/fuse
)

END()

RECURSE(
    protos
    vhost
    write_back_cache
)

RECURSE_FOR_TESTS(
    fuzz
    ut
)
