LIBRARY()

SRCS(
    authorizer.cpp
    hive_proxy.cpp
    ss_proxy.cpp
    user_stats.cpp
)

PEERDIR(
    cloud/storage/core/libs/kikimr

    ydb/core/base

    library/cpp/actors/core
)

END()
