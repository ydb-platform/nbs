UNITTEST_FOR(contrib/ydb/library/yql/providers/pq/async_io)

SIZE(MEDIUM)

SRCS(
    dq_pq_info_aggregator_ut.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    contrib/ydb/core/testlib/basics
    contrib/ydb/library/testlib/common
    contrib/ydb/library/yql/providers/pq/async_io
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
