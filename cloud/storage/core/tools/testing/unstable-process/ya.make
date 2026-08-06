PY3_PROGRAM(storage-unstable-process)

PEERDIR(
    contrib/python/requests/py3

    cloud/storage/core/tools/common/python
)

PY_SRCS(
    __main__.py
)

END()

RECURSE_FOR_TESTS(
    tests
)
