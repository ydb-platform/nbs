PY23_LIBRARY()

IF (PYTHON2)
    PEERDIR(contrib/ydb/public/sdk/python2)
ELSE()
    PEERDIR(contrib/ydb/public/sdk/python3)
ENDIF()

END()
