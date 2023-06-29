LIBRARY()

GENERATE_ENUM_SERIALIZATION(channel_data_kind.h)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/storage/core/libs/tablet/model
    library/cpp/actors/core
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
