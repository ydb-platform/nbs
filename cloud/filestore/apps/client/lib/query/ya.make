LIBRARY(filestore-libs-storage-query)

SRCS(
    query.cpp
    schema.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
