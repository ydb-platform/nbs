LIBRARY()

SRCS(
    db_pool.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/monlib/dynamic_counters
    contrib/ydb/library/db_pool/protos
    contrib/ydb/library/security
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

END()

