LIBRARY(topic)

CFLAGS(
    -Wno-deprecated-declarations
)

SRCS(
    topic_read.cpp
    topic_write.cpp
)

PEERDIR(
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public
    contrib/ydb/public/sdk/cpp/client/ydb_topic
)

GENERATE_ENUM_SERIALIZATION(topic_metadata_fields.h)

END()

RECURSE_FOR_TESTS(
    ut
)
