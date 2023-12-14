PY23_LIBRARY()

PY_SRCS(
    __init__.py
)

IF (PYTHON2)
    ENV(PYTHON2_YDB_IMPORT='yes')
    PEERDIR(contrib/ydb/public/sdk/python)
ELSE()
    PEERDIR(contrib/ydb/public/sdk/python)
ENDIF()

PEERDIR(
    contrib/ydb/tests/oss/canonical
)

END()
