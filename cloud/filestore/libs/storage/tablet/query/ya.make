LIBRARY(cloud-filestore-libs-storage-tablet-query)

SRCS(
    lexer.l
    parser.y
    query.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
