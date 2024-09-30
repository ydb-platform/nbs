LIBRARY()

SRCS(
    ss_proxy_actor.cpp
    ss_proxy_actor_describescheme.cpp
    ss_proxy_actor_modifyscheme.cpp
    ss_proxy_actor_waitschemetx.cpp
)

PEERDIR(
    cloud/storage/core/libs/api
    cloud/storage/core/libs/kikimr

    contrib/ydb/core/base
    contrib/ydb/core/tablet
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tx_proxy

    contrib/ydb/library/actors/core
)

END()
