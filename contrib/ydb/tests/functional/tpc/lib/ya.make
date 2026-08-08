PY3_LIBRARY()

    PY_SRCS (
        conftest.py
    )

    PEERDIR(
        contrib/ydb/tests/library
        contrib/ydb/tests/olap/load/lib
    )

END()
