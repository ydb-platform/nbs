PY3_PROGRAM()

PY_SRCS(
    __main__.py
)

PEERDIR(
    cloud/blockstore/tests/python/lib
    cloud/tasks/test/common
    library/python/testing/recipe
)

END()
