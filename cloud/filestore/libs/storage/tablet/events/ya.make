LIBRARY()

GENERATE_ENUM_SERIALIZATION(tablet_private.h)

SRCS(
    tablet_private.cpp
)

PEERDIR(
    cloud/filestore/libs/service
    cloud/filestore/libs/storage/api
    cloud/filestore/libs/storage/core
    cloud/filestore/libs/storage/model
    cloud/filestore/libs/storage/tablet/model
    cloud/filestore/libs/storage/tablet/protos
    cloud/filestore/private/api/protos

    cloud/storage/core/libs/common

    contrib/ydb/core/base
)

END()
