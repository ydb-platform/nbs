LIBRARY()

SRCS(
    authorizer.cpp
    hive_proxy.cpp
    ss_proxy.cpp
    user_stats.cpp
)

PEERDIR(
    cloud/storage/core/libs/kikimr
    cloud/storage/core/protos

    contrib/ydb/core/base

    contrib/ydb/library/actors/core
)

END()
