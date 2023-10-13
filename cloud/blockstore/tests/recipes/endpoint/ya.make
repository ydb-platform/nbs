PY3_PROGRAM(endpoint-recipe)

PY_SRCS(__main__.py)

PEERDIR(
    cloud/blockstore/public/sdk/python/client

    library/python/testing/recipe
)

END()
