LIBRARY()

SRCS(
    ss_proxy.cpp
    ss_proxy_actor.cpp
    ss_proxy_actor_createvolume.cpp
    ss_proxy_actor_describescheme.cpp
    ss_proxy_actor_describevolume.cpp
    ss_proxy_actor_modifyscheme.cpp
    ss_proxy_actor_modifyvolume.cpp
    ss_proxy_actor_waitschemetx.cpp
    ss_proxy_fallback_actor.cpp
    path_description_backup.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/ss_proxy/protos

    contrib/ydb/core/base
    contrib/ydb/core/tablet
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tx_proxy

    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
