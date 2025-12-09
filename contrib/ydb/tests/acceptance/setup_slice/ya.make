PY3_PROGRAM(setup_slice)

PY_MAIN(contrib.ydb.tests.acceptance.setup_slice:main)

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/ydb/tools/cfg
    contrib/ydb/tests/library
)

END()
