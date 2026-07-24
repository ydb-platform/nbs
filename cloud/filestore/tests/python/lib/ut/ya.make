PY3TEST()

TEST_SRCS(daemon_config_ut.py)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/tests/python/lib

    contrib/python/protobuf
    contrib/ydb/core/protos
)

END()
