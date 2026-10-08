LIBRARY(filestore-libs-storage-disk_registry_proxy-api)

SRCS(
    service.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/api
    cloud/filestore/private/api/protos

    cloud/storage/core/protos

    contrib/ydb/library/actors/core
)

END()
