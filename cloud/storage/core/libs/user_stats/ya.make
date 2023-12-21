LIBRARY()

SRCS(
    user_stats.cpp
    user_stats_actor.cpp
)

PEERDIR(
    cloud/storage/core/libs/user_stats/counter
    cloud/storage/core/protos

    library/cpp/actors/core

    ydb/core/base
    ydb/core/mon
)

END()
