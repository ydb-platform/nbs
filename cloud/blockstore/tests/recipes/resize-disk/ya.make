PY3_PROGRAM(resize-disk-recipe)

PY_SRCS(__main__.py)

PEERDIR(
    cloud/blockstore/public/sdk/python/client

    library/python/testing/recipe
)

END()
