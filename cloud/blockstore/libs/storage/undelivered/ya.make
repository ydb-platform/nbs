LIBRARY()

SRCS(
    undelivered.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
