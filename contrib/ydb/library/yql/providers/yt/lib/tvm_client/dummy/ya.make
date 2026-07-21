LIBRARY()

SRCS(
    dummy_tvm_client.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/lib/tvm_client
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/utils
)

END()
