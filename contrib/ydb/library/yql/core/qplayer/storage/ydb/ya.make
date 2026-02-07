LIBRARY()

SRCS(
    yql_qstorage_ydb.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/qplayer/storage/interface
    contrib/ydb/library/yql/core/qplayer/storage/memory
    contrib/ydb/public/sdk/cpp/client/ydb_table
    library/cpp/digest/old_crc
)

END()

RECURSE_FOR_TESTS(
    ut
)
