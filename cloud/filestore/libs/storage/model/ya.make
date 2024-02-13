LIBRARY(filestore-libs-storage-model)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

GENERATE_ENUM_SERIALIZATION(
    channel_data_kind.h
)

SRCS(
    channel_data_kind.cpp
)

END()
