LIBRARY()

OWNER(
    g:kikimr
)

SRCS(
    actor.cpp
    events.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/protos
    contrib/ydb/core/tablet_flat
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/actors/testlib/common
)

END()
