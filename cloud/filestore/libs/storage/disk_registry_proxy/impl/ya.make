LIBRARY(filestore-libs-storage-disk_registry_proxy-impl)

SRCS(
    disk_registry_proxy.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/api
    cloud/filestore/libs/storage/core
    cloud/filestore/libs/storage/disk_registry_proxy/api

    cloud/storage/core/libs/actors
    cloud/storage/core/libs/api
    cloud/storage/core/libs/kikimr

    contrib/ydb/core/base
    contrib/ydb/core/tablet
    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
