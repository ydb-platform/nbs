LIBRARY()

SRCS(
    device_integrity_check_actor.cpp
    direct_copy_actor.cpp
    io_request_parser.cpp
    multi_agent_write_device_blocks_actor.cpp
    multi_agent_write_handler.cpp
    session_cache_actor.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/local_nvme/protos
    cloud/blockstore/libs/nvme
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
