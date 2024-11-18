PY3_PROGRAM(storage-unstable-process)

PEERDIR(
    contrib/python/requests/py3

    cloud/storage/core/tools/common/python

    library/python/testing/yatest_common
)

PY_SRCS(
    __main__.py
)

END()
