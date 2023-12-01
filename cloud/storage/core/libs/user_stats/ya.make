LIBRARY()

SRCS(
    user_stats.cpp
    user_stats_actor.cpp
)

PEERDIR(
    cloud/storage/core/libs/user_stats/counter
    cloud/storage/core/protos

    contrib/ydb/library/actors/core

    contrib/ydb/core/base
    contrib/ydb/core/mon
)

END()
