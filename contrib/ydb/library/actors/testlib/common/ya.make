LIBRARY()

OWNER(
    g:kikimr
)

SRCS(
    events_scheduling.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
)

END()
