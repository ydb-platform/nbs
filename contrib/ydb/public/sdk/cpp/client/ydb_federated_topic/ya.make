LIBRARY()

GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/client/ydb_federated_topic/federated_topic.h)

SRCS(
    federated_topic.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_topic
    contrib/ydb/public/sdk/cpp/client/ydb_federated_topic/impl
)

END()

RECURSE_FOR_TESTS(
    ut
)
