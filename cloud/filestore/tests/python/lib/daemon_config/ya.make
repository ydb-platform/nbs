PY3TEST()

TEST_SRCS(test.py)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/tests/python/lib

    contrib/python/protobuf
    contrib/ydb/core/protos
)

END()
