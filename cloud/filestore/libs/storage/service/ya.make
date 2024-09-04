LIBRARY(filestore-libs-storage-service)

SRCS(
    helpers.cpp
    service.cpp
    service_actor.cpp
    service_actor_actions_change_storage_config.cpp
    service_actor_actions_configure_as_follower.cpp
    service_actor_actions_configure_followers.cpp
    service_actor_actions_describe_sessions.cpp
    service_actor_actions_drain_tablets.cpp
    service_actor_actions_forced_operation.cpp
    service_actor_actions_get_storage_config_fields.cpp
    service_actor_actions_get_storage_config.cpp
    service_actor_actions_reassign_tablet.cpp
    service_actor_actions_write_compaction_map.cpp
    service_actor_actions.cpp
    service_actor_alterfs.cpp
    service_actor_complete.cpp
    service_actor_createfs.cpp
    service_actor_createhandle.cpp
    service_actor_createnode.cpp
    service_actor_createsession.cpp
    service_actor_describefsmodel.cpp
    service_actor_destroyfs.cpp
    service_actor_destroysession.cpp
    service_actor_forward.cpp
    service_actor_fsync.cpp
    service_actor_getfsinfo.cpp
    service_actor_getnodeattr.cpp
    service_actor_getsessionevents.cpp
    service_actor_list.cpp
    service_actor_listnodes.cpp
    service_actor_monitoring.cpp
    service_actor_monitoring_search.cpp
    service_actor_ping.cpp
    service_actor_pingsession.cpp
    service_actor_readdata.cpp
    service_actor_statfs.cpp
    service_actor_update_stats.cpp
    service_actor_writedata.cpp
    service_state.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/storage/api
    cloud/filestore/libs/storage/core
    cloud/filestore/libs/storage/model

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    contrib/ydb/library/actors/core
    library/cpp/monlib/service/pages
    library/cpp/string_utils/quote

    contrib/ydb/core/base
    contrib/ydb/core/mind
    contrib/ydb/core/mon
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
)

END()

RECURSE_FOR_TESTS(
    ut
)
