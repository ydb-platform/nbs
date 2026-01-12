LIBRARY(virtiofsd)

LICENSE(
    "(GPL-1.0-or-later OR BSD-3-Clause)" AND
    "(GPL-2.0-only WITH Linux-syscall-note OR BSD-2-Clause)" AND
    BSD-2-Clause AND
    LGPL-2.0-only AND
    LGPL-2.1-only
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(3.2)

BUILD_ONLY_IF(WARNING OS_LINUX)

NO_UTIL()

NO_RUNTIME()

SRCDIR(cloud/contrib/virtiofsd)

SRCS(
    buffer.c
    fuse_log.c
    fuse_lowlevel.c
    fuse_opt.c
    fuse_signals.c
)

END()
