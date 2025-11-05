LIBRARY()

SRCS(
    manager.h
    manager.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/services
    contrib/ydb/library/yql/providers/common/metrics
    contrib/ydb/library/yql/utils
)

END()

RECURSE_FOR_TESTS(
    style
)
