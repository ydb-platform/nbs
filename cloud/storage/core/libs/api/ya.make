LIBRARY()

SRCS(
    authorizer.cpp
    hive_proxy.cpp
    user_stats.cpp
)

PEERDIR(
    cloud/storage/core/libs/kikimr
    library/cpp/actors/core
    contrib/ydb/core/base
)

END()
