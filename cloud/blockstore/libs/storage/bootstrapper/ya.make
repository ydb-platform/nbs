LIBRARY()

SRCS(
    bootstrapper.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    library/cpp/actors/core
    ydb/core/base
    ydb/core/tablet
)

END()

RECURSE_FOR_TESTS(
    ut
)
