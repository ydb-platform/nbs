LIBRARY()

SRCS(
    shard_reader.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/kqp/compute_actor
)

END()
