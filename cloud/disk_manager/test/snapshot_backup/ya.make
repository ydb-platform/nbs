PY3_LIBRARY()

PY_SRCS(
    _zstd.pyx
    __init__.py
    __main__.py
    cloud.py
    config.py
    devices.py
    reader.py
    runner.py
    state.py
    transport.py
)

PEERDIR(
    contrib/libs/zstd
    contrib/python/cryptography/py3
    contrib/python/lz4/py3
)

ADDINCL(contrib/libs/zstd/include)

END()

RECURSE_FOR_TESTS(tests)
