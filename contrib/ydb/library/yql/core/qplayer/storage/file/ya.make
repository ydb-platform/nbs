LIBRARY()

SRCS(
    yql_qstorage_file.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/qplayer/storage/interface
    contrib/ydb/library/yql/core/qplayer/storage/memory
    library/cpp/digest/old_crc
)

END()

RECURSE_FOR_TESTS(
    ut
)
