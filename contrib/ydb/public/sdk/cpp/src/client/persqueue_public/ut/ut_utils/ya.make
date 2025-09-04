LIBRARY()

SRCS(
    data_plane_helpers.h
    data_plane_helpers.cpp
    sdk_test_setup.h
    test_utils.h
    test_server.h
    test_server.cpp
    ut_utils.h
    ut_utils.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/server
    library/cpp/testing/unittest
    library/cpp/threading/chunk_queue
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/src/library/persqueue/topic_parser_public
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public
    contrib/ydb/public/sdk/cpp/src/client/table
)

YQL_LAST_ABI_VERSION()

END()
