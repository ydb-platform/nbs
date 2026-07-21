LIBRARY()

SRCS(
    actor.cpp
    task.cpp
)

PEERDIR(
    library/cpp/retry
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
    contrib/ydb/core/tablet_flat
)

END()

RECURSE_FOR_TESTS(
    ut
)
