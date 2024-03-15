LIBRARY()

SRCS(
    forward_read_marked.cpp
    forward_write_and_mark_used.cpp
    read_disk_registry_based_overlay.cpp
    shadow_disk_actor.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/partition_common
    cloud/blockstore/libs/storage/protos
    cloud/blockstore/libs/storage/protos_ydb

    library/cpp/actors/core
    library/cpp/lwtrace
)

END()

RECURSE()

RECURSE_FOR_TESTS(
    ut
)
