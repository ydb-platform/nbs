LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/testlib/pg
    contrib/ydb/core/tx/replication/ydb_proxy
    contrib/ydb/library/actors/core
    contrib/ydb/public/sdk/cpp/src/client/topic
    library/cpp/testing/unittest
)

SRCS(
    mock_service.cpp
    test_env.h
    test_table.cpp
    test_topic.cpp
    write_topic.h
)

YQL_LAST_ABI_VERSION()

END()
