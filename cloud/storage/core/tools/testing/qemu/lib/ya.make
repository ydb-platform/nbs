PY3_LIBRARY()

PY_SRCS(
    common.py
    __init__.py
    qemu_with_migration.py
    qemu.py
    qmp.py
    recipe.py
)

PEERDIR(
    contrib/python/PyYAML
    contrib/python/retrying
    library/python/fs
    library/python/testing/recipe
    ydb/tests/library
)

END()
