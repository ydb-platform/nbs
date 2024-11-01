PY3_PROGRAM()

PY_SRCS(
    __main__.py
    node_launcher.py
)

PEERDIR(
    cloud/tasks/test/common
    contrib/ydb/tests/library
    cloud/blockstore/pylibs/ydb/tests/library
    library/python/testing/recipe
)

END()

RECURSE(
    init-db
    node
    tasks
)
