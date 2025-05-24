LIBRARY()

SRCS(
    stats_fetcher.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr
)

END()

RECURSE_FOR_TESTS(
    ut
)
