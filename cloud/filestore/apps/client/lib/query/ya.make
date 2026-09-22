LIBRARY(filestore-apps-client-query)

SRCS(
    parser.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
