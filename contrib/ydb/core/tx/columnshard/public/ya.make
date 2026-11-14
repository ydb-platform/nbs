LIBRARY()

SRCS(
    events.cpp
)

GENERATE_ENUM_SERIALIZATION(events.h)

PEERDIR(
    contrib/ydb/core/control
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/tx
    contrib/ydb/library/actors/core
    contrib/ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
