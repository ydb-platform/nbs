LIBRARY()

LICENSE(
    BSD-2-Clause AND
    GPL-1.0-or-later AND
    GPL-2.0-only WITH Linux-syscall-note AND
    LGPL-2.0-only AND
    LGPL-2.1-only
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(3.2)

BUILD_ONLY_IF(OS_LINUX)

NO_UTIL()

NO_RUNTIME()

SRCS(
    buffer.c
    fuse_log.c
    fuse_lowlevel.c
    fuse_opt.c
    fuse_signals.c
)

END()
