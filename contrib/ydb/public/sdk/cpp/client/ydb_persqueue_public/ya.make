LIBRARY()

SRCS(
    persqueue.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_core
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
)

END()
