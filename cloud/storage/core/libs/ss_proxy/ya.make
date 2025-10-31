LIBRARY()

SRCS(
    path_description_backup.cpp
    ss_proxy.cpp
    ss_proxy_actor.cpp
    ss_proxy_actor_describescheme.cpp
    ss_proxy_actor_modifyscheme.cpp
    ss_proxy_actor_waitschemetx.cpp
    ss_proxy_fallback_actor.cpp
)

PEERDIR(
    cloud/storage/core/libs/ss_proxy/protos

    cloud/storage/core/libs/actors
    cloud/storage/core/libs/api
    cloud/storage/core/libs/kikimr

    ydb/core/base
    ydb/core/tablet
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_proxy

    ydb/library/actors/core
)

END()
