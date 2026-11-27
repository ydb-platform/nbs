LIBRARY(cloud-filestore-libs-storage-tablet-query)

SRCS(
    executor.cpp
    lexer.l
    parser.y
    query.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    contrib/ydb/core/tablet_flat
)

END()

RECURSE_FOR_TESTS(
    ut
)
