LIBRARY()

SRCS(
    managed_executor.cpp
    managed_executor.h
    topic_sdk_test_setup.cpp
    topic_sdk_test_setup.h
    trace.cpp
    trace.h
)

PEERDIR(
    contrib/ydb/library/grpc/server
    library/cpp/testing/unittest
    library/cpp/threading/chunk_queue
    contrib/ydb/core/testlib/default
    contrib/ydb/library/persqueue/topic_parser_public
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_topic
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()
