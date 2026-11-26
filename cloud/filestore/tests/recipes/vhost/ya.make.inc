PY3_PROGRAM(vhost-recipe)

PY_SRCS(
    cloud/filestore/tests/recipes/vhost/__main__.py
)

PEERDIR(
    cloud/filestore/tests/python/lib
    cloud/storage/core/tests/common
    cloud/storage/core/tests/ydb

    library/python/testing/recipe
    library/python/testing/yatest_common
)

DEPENDS(
    cloud/filestore/apps/vhost
)

END()
