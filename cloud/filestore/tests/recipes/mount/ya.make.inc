PY3_PROGRAM(mount-recipe)

PY_SRCS(
    cloud/filestore/tests/recipes/mount/__main__.py
)

PEERDIR(
    cloud/filestore/tests/python/lib

    library/python/testing/recipe
    library/python/testing/yatest_common
)

END()
