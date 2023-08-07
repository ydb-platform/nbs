LIBRARY()

SRCS(
    restore_validator_actor.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/disk_registry/model

    cloud/storage/core/libs/actors

    library/cpp/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
