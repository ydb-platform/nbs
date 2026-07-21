GTEST()


SRCS(
    consumer_offset_tracker_ut.cpp
    message_id_deduplicator_ut.cpp
)

PEERDIR(
    contrib/ydb/core/persqueue/pqtablet/partition
    contrib/ydb/core/persqueue/common
    contrib/ydb/core/protos
)

END()
