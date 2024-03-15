LIBRARY()

SRCS(
    session_cache_actor.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/disk_agent/model

    cloud/storage/core/libs/actors
    cloud/storage/core/protos

    library/cpp/actors/core
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
)
