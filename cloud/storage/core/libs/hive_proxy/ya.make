LIBRARY()

SRCS(
    hive_proxy.cpp
    hive_proxy_actor.cpp
    hive_proxy_actor_bootext.cpp
    hive_proxy_actor_create.cpp
    hive_proxy_actor_drain.cpp
    hive_proxy_actor_getinfo.cpp
    hive_proxy_actor_lock.cpp
    hive_proxy_actor_reassign.cpp
    hive_proxy_actor_unlock.cpp
    hive_proxy_fallback_actor.cpp
    tablet_boot_info_backup.cpp
)

PEERDIR(
    cloud/storage/core/libs/hive_proxy/protos

    cloud/storage/core/libs/actors
    cloud/storage/core/libs/api
    cloud/storage/core/libs/kikimr

    ydb/core/base
    ydb/core/mind
    ydb/core/tablet
    ydb/core/tablet_flat

    ydb/library/actors/core

    library/cpp/monlib/dynamic_counters
)

END()

RECURSE(
    protos
)

RECURSE_FOR_TESTS(
    ut
)
