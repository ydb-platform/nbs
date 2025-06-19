LIBRARY()

GENERATE_ENUM_SERIALIZATION(propagate_to_follower.h)

SRCS(
    create_volume_link_actor.cpp
    forward_write_and_mark_used.cpp
    propagate_to_follower.cpp
    read_disk_registry_based_overlay.cpp
    release_devices_actor.cpp
    shadow_disk_actor.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/partition_common
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
