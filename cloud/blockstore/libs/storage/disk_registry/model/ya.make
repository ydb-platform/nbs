LIBRARY()

#INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

SRCS(
    agent_counters.cpp
    agent_list.cpp
    device_list.cpp
    device_replacement_tracker.cpp
    pending_cleanup.cpp
    replica_table.cpp
    user_notification.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/protos

    cloud/storage/core/libs/diagnostics
)

END()

RECURSE_FOR_TESTS(ut)
