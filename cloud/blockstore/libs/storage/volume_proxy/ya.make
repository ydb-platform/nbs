LIBRARY()

SRCS(
    volume_proxy.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/storage/core/libs/api
    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
