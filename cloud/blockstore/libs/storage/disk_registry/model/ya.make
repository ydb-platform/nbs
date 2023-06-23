LIBRARY()

SRCS(
    agent_counters.cpp
    agent_list.cpp
    device_list.cpp
    pending_cleanup.cpp
    replica_table.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/protos

    cloud/storage/core/libs/diagnostics
)

END()

RECURSE_FOR_TESTS(ut)
