PY3_PROGRAM(ydb-recipe)

PY_SRCS(__main__.py)

PEERDIR(
    cloud/storage/core/tests/ydb

    library/python/testing/recipe
    library/python/testing/yatest_common
)

END()
