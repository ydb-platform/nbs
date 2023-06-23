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
    contrib/python/qemu
    contrib/python/retrying
    devtools/ya/core/config
    kikimr/ci/libraries
    library/python/fs
    library/python/testing/recipe
)

END()
