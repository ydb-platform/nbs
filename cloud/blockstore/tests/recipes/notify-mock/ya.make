PY3_PROGRAM(notify-mock-recipe)

PY_SRCS(__main__.py)

PEERDIR(
    cloud/blockstore/tests/python/lib

    library/python/testing/recipe
    library/python/testing/yatest_common

    contrib/python/requests/py3
)

FILES(
    start.sh
    stop.sh
)

END()
