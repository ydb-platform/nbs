UNITTEST_FOR(contrib/ydb/library/yql/providers/pq/async_io)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

SRCS(
    dq_pq_rd_read_actor_ut.cpp
    dq_pq_read_actor_ut.cpp
    dq_pq_write_actor_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/basics/default
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/computation/llvm16
    yql/essentials/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/common/ut_helpers
    contrib/ydb/library/yql/providers/pq/gateway/native
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql
    contrib/ydb/public/sdk/cpp/src/client/datastreams
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public
    contrib/ydb/tests/fq/pq_async_io
)

YQL_LAST_ABI_VERSION()

END()
