LIBRARY()

SRCS(
    volume_proxy.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    contrib/ydb/library/actors/core
    contrib/ydb/core/tablet
)

END()

RECURSE_FOR_TESTS(
    ut
)
