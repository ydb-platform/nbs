PY3_PROGRAM(vhost-restore-endpoint-recipe)

PY_SRCS(
    cloud/filestore/tests/recipes/vhost-restore-endpoint/__main__.py
)

PEERDIR(
    cloud/filestore/tests/python/lib

    library/python/testing/recipe
    library/python/testing/yatest_common
)

END()
