LIBRARY()

GENERATE_ENUM_SERIALIZATION(events_private.h)

SRCS(
    actor_checkrange.cpp
    actor_read_blob.cpp
    actor_describe_base_disk_blocks.cpp
    actor_loadfreshblobs.cpp
    actor_trimfreshlog.cpp
    drain_actor_companion.cpp
    long_running_operation_companion.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/partition_common/model
    cloud/storage/core/libs/common
    cloud/storage/core/libs/kikimr
    ydb/library/actors/core
    ydb/core/base
)

END()

RECURSE(
    model
)

RECURSE_FOR_TESTS(
    ut
)
