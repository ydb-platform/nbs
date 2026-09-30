LIBRARY(filestore-libs-storage-dr_proxy-api)

SRCS(
    service.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/api
    cloud/filestore/private/api/protos

    contrib/ydb/library/actors/core
)

END()
