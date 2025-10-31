LIBRARY(filestore-libs-storage-ss_proxy)

SRCS(
    path.cpp
    ss_proxy.cpp
    ss_proxy_actor.cpp
    ss_proxy_actor_alterfs.cpp
    ss_proxy_actor_createfs.cpp
    ss_proxy_actor_describefs.cpp
    ss_proxy_actor_destroyfs.cpp
    ss_proxy_fallback_actor.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/api
    cloud/filestore/libs/storage/core

    cloud/storage/core/libs/kikimr
    cloud/storage/core/libs/ss_proxy

    ydb/core/base
    ydb/core/tablet
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_proxy

    ydb/library/actors/core

    library/cpp/string_utils/quote
)

END()

RECURSE_FOR_TESTS(
    ut
)
