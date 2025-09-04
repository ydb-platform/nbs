LIBRARY()

SRCS(
    topic_sdk_test_setup.cpp
    topic_sdk_test_setup.h
)

PEERDIR(
    contrib/ydb/library/grpc/server
    library/cpp/testing/unittest
    library/cpp/threading/chunk_queue
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/src/library/persqueue/topic_parser_public
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/topic

    contrib/ydb/public/sdk/cpp/tests/integration/topic/utils
)

YQL_LAST_ABI_VERSION()

END()
