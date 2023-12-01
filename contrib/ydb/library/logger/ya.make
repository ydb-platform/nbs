LIBRARY()

OWNER(g:kikimr)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/logger
)

SRCS(
    actor.cpp
)

END()
