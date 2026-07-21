LIBRARY()

SRCS(
    batch_processor.cpp
    batch_cutter.cpp
    consumer_batch_processor.cpp
)

PEERDIR(
    library/cpp/streams/zstd
    contrib/ydb/core/persqueue/common
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/public/codecs
    contrib/ydb/public/sdk/cpp/src/library/kafka
    contrib/ydb/library/persqueue/counter_time_keeper
    contrib/ydb/core/persqueue/public/write_meta
    contrib/ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
