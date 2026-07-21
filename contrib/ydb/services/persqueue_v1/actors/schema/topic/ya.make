LIBRARY()

PEERDIR(
    contrib/ydb/core/persqueue/public/schema
    contrib/ydb/services/persqueue_v1/actors/schema/common
)

SRCS(
    actors.cpp
    alter_topic.cpp
    create_topic.cpp
    describe_consumer.cpp
    describe_partition.cpp
    drop_topic.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
