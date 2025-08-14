PY3_PROGRAM(endpoint-two-clusters-recipe)

PY_SRCS(__main__.py)

PEERDIR(
    cloud/blockstore/public/sdk/python/client

    library/python/testing/recipe
)

END()
