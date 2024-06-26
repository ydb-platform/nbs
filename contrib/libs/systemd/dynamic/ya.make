DYNAMIC_LIBRARY()

LICENSE(
    CC0-1.0 AND
    LGPL-2.0-or-later AND
    LGPL-2.1-only AND
    LGPL-2.1-or-later AND
    MIT AND
    Public-Domain
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

DYNAMIC_LIBRARY_FROM(contrib/libs/systemd/static)

LICENSE_RESTRICTION_EXCEPTIONS(contrib/libs/systemd/static)

NO_RUNTIME()

EXPORTS_SCRIPT(libsystemd.exports)

END()
