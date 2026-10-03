PY3_PROGRAM(ydb_recipe)

PY_SRCS(__main__.py)

PEERDIR(
    cloud/tasks/test/common
    contrib/ydb/tests/library
    library/python/testing/recipe
)

END()
