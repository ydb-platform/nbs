LIBRARY(filestore-libs-storage-ss_proxy)

SRCS(
    path.cpp
    ss_proxy.cpp
    ss_proxy_actor.cpp
    ss_proxy_actor_alterfs.cpp
    ss_proxy_actor_createfs.cpp
    ss_proxy_actor_describefs.cpp
    ss_proxy_actor_describescheme.cpp
    ss_proxy_actor_destroyfs.cpp
    ss_proxy_actor_modifyscheme.cpp
    ss_proxy_actor_waitschemetx.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/api
    cloud/filestore/libs/storage/core
    cloud/storage/core/libs/kikimr
    contrib/ydb/library/actors/core
    library/cpp/string_utils/quote
    contrib/ydb/core/base
    contrib/ydb/core/tablet
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tx_proxy
)

END()

RECURSE_FOR_TESTS(
    ut
)
