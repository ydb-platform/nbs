PY3_LIBRARY()

PY_SRCS(
    __init__.py
    runner.py
)

PEERDIR(
    library/python/testing/yatest_common
    contrib/ydb/tests/library
)

END()
