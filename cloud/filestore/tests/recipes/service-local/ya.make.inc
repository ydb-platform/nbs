PY3_PROGRAM(service-local-recipe)

PY_SRCS(
    cloud/filestore/tests/recipes/service-local/__main__.py
)

PEERDIR(
    cloud/filestore/tests/python/lib
    cloud/storage/core/tests/common
    library/python/testing/recipe
    library/python/testing/yatest_common
)

END()
