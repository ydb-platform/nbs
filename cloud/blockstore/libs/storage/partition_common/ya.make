LIBRARY()

GENERATE_ENUM_SERIALIZATION(events_private.h)

SRCS(
    actor_checkrange.cpp
    actor_read_blob.cpp
    actor_describe_base_disk_blocks.cpp
    actor_loadfreshblobs.cpp
    actor_trimfreshlog.cpp
    commit_ids_state.cpp
    drain_actor_companion.cpp
    fresh_blocks_companion_initfreshblocks.cpp
    fresh_blocks_companion.cpp
    io_companion_patchblob.cpp
    io_companion_readblob.cpp
    io_companion_writeblobs.cpp
    io_companion.cpp
    long_running_operation_companion.cpp
    part_channels_state.cpp
    part_fresh_blocks_state.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/partition_common/model
    cloud/storage/core/libs/common
    cloud/storage/core/libs/kikimr
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
)

END()

RECURSE(
    model
)

RECURSE_FOR_TESTS(
    ut
)
