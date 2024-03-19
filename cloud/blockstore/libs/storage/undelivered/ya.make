LIBRARY()

SRCS(
    undelivered.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    library/cpp/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
