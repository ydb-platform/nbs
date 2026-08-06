PY3_PROGRAM(blockstore-disk-agent-recipe)

PY_SRCS(
    cloud/filestore/tests/recipes/blockstore-disk-agent/__main__.py
)

PEERDIR(
    cloud/blockstore/public/sdk/python/client
    cloud/blockstore/public/sdk/python/protos
    cloud/blockstore/tests/python/lib

    cloud/filestore/tests/python/lib

    library/python/testing/recipe
    library/python/testing/yatest_common
)

END()
