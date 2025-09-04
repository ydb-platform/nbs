UNITTEST_FOR(contrib/ydb/core/fq/libs/row_dispatcher)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

SRCS(
    coordinator_ut.cpp
    leader_election_ut.cpp
    row_dispatcher_ut.cpp
    topic_session_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/fq/libs/row_dispatcher/format_handler/ut/common
    contrib/ydb/core/fq/libs/row_dispatcher
    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/actors
    contrib/ydb/library/testlib/pq_helpers
    contrib/ydb/library/yql/providers/pq/gateway/dummy
    contrib/ydb/tests/fq/pq_async_io
    yql/essentials/sql/pg_dummy
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
