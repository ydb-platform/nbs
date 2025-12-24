UNITTEST_FOR(contrib/ydb/library/yql/dq/actors/common)

SRCS(
    retry_events_queue_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/testlib/actors
    contrib/ydb/core/testlib
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
