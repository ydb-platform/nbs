PY3_LIBRARY()

PY_SRCS(
    fqrun.py
    kqprun.py
)

PEERDIR(
    contrib/ydb/library/yql/tests/common/test_framework
)

END()
