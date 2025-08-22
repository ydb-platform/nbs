LIBRARY()

GENERATE_ENUM_SERIALIZATION(follower_disk_actor.h)
GENERATE_ENUM_SERIALIZATION(propagate_to_follower.h)
GENERATE_ENUM_SERIALIZATION(volume_as_partition_actor.h)

SRCS(
    create_volume_link_actor.cpp
    follower_disk_actor.cpp
    forward_write_and_mark_used.cpp
    partition_statistics_collector_actor.cpp
    propagate_to_follower.cpp
    read_disk_registry_based_overlay.cpp
    release_devices_actor.cpp
    shadow_disk_actor.cpp
    volume_as_partition_actor.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/partition_common
    cloud/blockstore/libs/storage/partition_nonrepl
    cloud/blockstore/libs/storage/protos
    cloud/blockstore/libs/storage/protos_ydb

    contrib/ydb/library/actors/core
    library/cpp/lwtrace
)

END()

RECURSE()

RECURSE_FOR_TESTS(
    ut
)
