PY23_LIBRARY()

PY_SRCS(
    __init__.py
)

IF (PYTHON2)
    ENV(PYTHON2_YDB_IMPORT='yes')
    PEERDIR(contrib/ydb/public/sdk/python2)
ELSE()
    PEERDIR(contrib/ydb/public/sdk/python3)
ENDIF()

PEERDIR(
    contrib/ydb/tests/oss/canonical
)

END()
