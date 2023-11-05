LIBRARY()

SRCS(
    time_cast.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/core/tx
)

END()

RECURSE_FOR_TESTS(
    ut
)
