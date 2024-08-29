LIBRARY(filestore-libs-storage-model)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

GENERATE_ENUM_SERIALIZATION(
    channel_data_kind.h
)

SRCS(
    block_buffer.cpp
    channel_data_kind.cpp
    utils.cpp
)

PEERDIR(
    cloud/storage/core/libs/common

    contrib/libs/protobuf
)

END()
