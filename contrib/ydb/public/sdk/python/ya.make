PY23_LIBRARY()

# use direct include py2/py3 for skip protobuf peerdirs from contrib/python/ydb
IF (PYTHON2)
    PEERDIR(contrib/python/ydb/py2)
ELSE()
    PEERDIR(contrib/python/ydb/py3)
ENDIF()

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
)

END()
