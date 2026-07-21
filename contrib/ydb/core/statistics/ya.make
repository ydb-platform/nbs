LIBRARY()

SRCS(
    events.h
)

PEERDIR(
    util
    contrib/ydb/library/actors/core
    contrib/ydb/library/query_actor
    contrib/ydb/library/yql/core/minsketch
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
)

END()

RECURSE(
    aggregator
    common
    database
    service
    ut_common
)

RECURSE_FOR_TESTS(
    aggregator/ut
    database/ut
    service/ut
)
