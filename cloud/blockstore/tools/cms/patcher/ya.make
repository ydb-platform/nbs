PY3_PROGRAM(blockstore-patcher)

PEERDIR(
    cloud/blockstore/tools/cms/lib
)

PY_SRCS(
    __main__.py
)

END()

RECURSE_FOR_TESTS(
    tests
)
