LIBRARY()

SRCS(
    bootstrapper.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/tablet
)

END()

RECURSE_FOR_TESTS(
    ut
)
