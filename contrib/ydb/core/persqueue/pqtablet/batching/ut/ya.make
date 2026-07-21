UNITTEST_FOR(contrib/ydb/core/persqueue/pqtablet/batching)

SRCS(
    batch_cutter_ut.cpp
)

PEERDIR(
    library/cpp/streams/zstd
    contrib/ydb/core/persqueue/public/write_meta
    contrib/ydb/core/protos
    contrib/ydb/public/sdk/cpp/src/library/kafka
)

END()
