LIBRARY()

SRCS(
    volume_throttling_manager.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/service
    cloud/blockstore/libs/storage/volume_throttling_manager/model

    cloud/storage/core/libs/actors
    cloud/storage/core/libs/common

    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
