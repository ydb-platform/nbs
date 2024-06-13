LIBRARY()

OWNER(
    monster
    g:kikimr
)

SRCS(
    events.h
    stat_service.h
    stat_service.cpp
)

PEERDIR(
    util
    contrib/ydb/library/actors/core
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
)

END()

RECURSE(
    aggregator
)

RECURSE_FOR_TESTS(
    ut
)
