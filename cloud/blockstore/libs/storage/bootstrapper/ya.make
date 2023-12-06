LIBRARY()

SRCS(
    bootstrapper.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    ydb/library/actors/core
    ydb/core/base
    ydb/core/tablet
)

END()

RECURSE_FOR_TESTS(
    ut
)
