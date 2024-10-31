PY3_PROGRAM(access-service-recipe)

PY_SRCS(__main__.py)

DEPENDS(
    cloud/storage/core/tools/testing/access_service/mock
)

PEERDIR(
    cloud/storage/core/tools/testing/access_service/lib

    contrib/ydb/tests/library
    cloud/blockstore/pylibs/ydb/tests/library

    library/python/testing/recipe
)

END()
