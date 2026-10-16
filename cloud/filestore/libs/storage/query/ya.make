LIBRARY(cloud-filestore-libs-storage-query)

SRCS(
    parser.rl6
)

PEERDIR(
    cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
