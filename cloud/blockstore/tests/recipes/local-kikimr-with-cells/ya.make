PY3_PROGRAM(local-kikimr-with-cells-recipe)

PY_SRCS(__main__.py)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib

    cloud/storage/core/tests/common

    library/python/testing/recipe
    library/python/testing/yatest_common

    contrib/ydb/tests/library
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

END()
