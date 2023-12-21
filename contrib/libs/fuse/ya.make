LIBRARY()

LICENSE(
    BSD-2-Clause AND
    BSD-3-Clause AND
    GPL-1.0-or-later AND
    LGPL-2.0-only
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2.9.3)

BUILD_ONLY_IF(WARNING OS_LINUX)

ADDINCL(
    GLOBAL contrib/libs/fuse/include
    contrib/libs/libiconv/include
)

PEERDIR(
    contrib/libs/libiconv
)

NO_RUNTIME()

NO_COMPILER_WARNINGS()

CFLAGS(
    -fpic
    -D_FILE_OFFSET_BITS=64
    -DFUSE_USE_VERSION=29
)

SRCDIR(contrib/libs/fuse/lib)

SRCS(
    fuse.c
    fuse_mt.c
    fuse_kern_chan.c
    fuse_loop.c
    fuse_loop_mt.c
    fuse_lowlevel.c
    fuse_opt.c
    fuse_session.c
    fuse_signals.c
    mount.c
    mount_util.c
    buffer.c
    cuse_lowlevel.c
    helper.c
    modules/subdir.c
    modules/iconv.c
)

END()
