LIBRARY()

SRCS(
    actor_read_blob.cpp
    actor_read_blocks_from_base_disk.cpp
    actor_loadfreshblobs.cpp
    actor_trimfreshlog.cpp
    drain_actor_companion.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/partition_common/model
    cloud/storage/core/libs/common
    cloud/storage/core/libs/kikimr
    library/cpp/actors/core
    ydb/core/base
)

END()

RECURSE(
    model
)

RECURSE_FOR_TESTS(
    ut
)
