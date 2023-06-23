PY3_PROGRAM(notify-mock-recipe)

PY_SRCS(__main__.py)

PEERDIR(
    cloud/blockstore/tests/python/lib

    library/python/testing/recipe
    library/python/testing/yatest_common

    contrib/python/requests
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

END()
