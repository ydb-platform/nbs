LIBRARY()

SRCS(
    journalled_device_adapter.cpp
    journalled_device_manager.cpp
)

PEERDIR(
    cloud/blockstore/libs/service
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/disk_agent/model

    cloud/fastshard/libs/journal/iface
    cloud/fastshard/libs/journal/server

    cloud/storage/core/libs/common

    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
