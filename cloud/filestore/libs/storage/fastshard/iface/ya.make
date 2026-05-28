LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

SRCS(
    fs.cpp
)

PEERDIR(
    cloud/filestore/private/api/protos
    cloud/filestore/public/api/protos

    cloud/storage/core/libs/common
)

END()
