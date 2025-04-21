LIBRARY()

SRCS(
    device_health_check_actor.cpp
    direct_copy_actor.cpp
    io_request_parser.cpp
    session_cache_actor.cpp
    multi_agent_write_device_blocks_actor.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/disk_agent/model
    cloud/blockstore/libs/storage/protos

    cloud/storage/core/libs/actors
    cloud/storage/core/protos

    contrib/ydb/library/actors/core
    contrib/ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
