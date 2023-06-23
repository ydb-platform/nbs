LIBRARY()

SRCS(
    restore_validator_actor.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/api

    cloud/storage/core/libs/actors

    library/cpp/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
