UNITTEST_FOR(contrib/ydb/library/yql/providers/pq/async_io)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

SRCS(
    dq_pq_read_actor_ut.cpp
    dq_pq_write_actor_ut.cpp
    ut_helpers.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/basics/default
    contrib/ydb/library/yql/minikql/computation/llvm14
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/common/ut_helpers
    contrib/ydb/library/yql/sql
    contrib/ydb/public/sdk/cpp/client/ydb_datastreams
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public
    contrib/ydb/library/yql/minikql/comp_nodes/llvm14
)

YQL_LAST_ABI_VERSION()

END()
