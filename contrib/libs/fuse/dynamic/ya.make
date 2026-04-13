DYNAMIC_LIBRARY(fuse)

LICENSE(
    "(GPL-1.0-or-later OR BSD-3-Clause)" AND
    BSD-2-Clause AND
    LGPL-2.0-only
)

LICENSE_RESTRICTION_EXCEPTIONS(
    contrib/libs/fuse/static
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2.9.3)

DYNAMIC_LIBRARY_FROM(contrib/libs/fuse/static)

NO_RUNTIME()

END()
