LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/replication/ydb_proxy
    contrib/ydb/public/sdk/cpp/client/ydb_topic
    library/cpp/testing/unittest
)

SRCS(
    test_env.h
    test_table.cpp
    write_topic.h
)

YQL_LAST_ABI_VERSION()

END()
