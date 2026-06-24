LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

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
    contrib/ydb/public/sdk/cpp/src/library/persqueue/topic_parser_public
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/src/client/table
)

YQL_LAST_ABI_VERSION()

END()
