LIBRARY()

SRCS(
    user_stats.cpp
    user_stats_actor.cpp
)

PEERDIR(
    cloud/storage/core/libs/user_stats/counter
    cloud/storage/core/protos

    ydb/library/actors/core

    ydb/core/base
    ydb/core/mon
)

END()
