LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/deny_ydb_dependency.inc)

GENERATE_ENUM_SERIALIZATION(channel_data_kind.h)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/storage/core/libs/tablet/model
    contrib/ydb/library/actors/core
)

SRCS(
    channel_data_kind.cpp
    channel_permissions.cpp
    composite_task_waiter.cpp
    requests_in_progress.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
