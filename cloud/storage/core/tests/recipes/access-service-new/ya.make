PY3_PROGRAM(access-service-new-recipe)

PY_SRCS(__main__.py)

DEPENDS(
    cloud/storage/core/tools/testing/access_service_new/mock
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

PEERDIR(
    cloud/storage/core/tools/testing/access_service_new/lib
    cloud/storage/core/tests/common

    contrib/ydb/tests/library
    library/python/testing/recipe
)

END()
