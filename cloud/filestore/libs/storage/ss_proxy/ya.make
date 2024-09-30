LIBRARY(filestore-libs-storage-ss_proxy)

SRCS(
    path.cpp
    ss_proxy.cpp
    ss_proxy_actor.cpp
    ss_proxy_actor_alterfs.cpp
    ss_proxy_actor_createfs.cpp
    ss_proxy_actor_describefs.cpp
    ss_proxy_actor_destroyfs.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/api
    cloud/filestore/libs/storage/core

    cloud/storage/core/libs/kikimr
    cloud/storage/core/libs/ss_proxy

    contrib/ydb/core/base
    contrib/ydb/core/tablet
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tx_proxy

    contrib/ydb/library/actors/core

    library/cpp/string_utils/quote
)

END()

RECURSE_FOR_TESTS(
    ut
)
