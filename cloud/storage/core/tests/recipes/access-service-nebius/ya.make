PY3_PROGRAM(access-service-nebius-recipe)

PY_SRCS(__main__.py)

DEPENDS(
    cloud/storage/core/tools/testing/access_service_nebius/mock
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

PEERDIR(
    cloud/storage/core/tools/testing/access_service_nebius/lib
    contrib/ydb/tests/library
    library/python/testing/recipe
)

END()
