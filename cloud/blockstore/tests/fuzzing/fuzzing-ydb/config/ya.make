PY3_PROGRAM(config-recipe)

OWNER(g:cloud-nbs)

PY_SRCS(__main__.py)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib

    library/python/testing/recipe

    contrib/ydb/core/protos
    contrib/ydb/tests/library
)

END()
