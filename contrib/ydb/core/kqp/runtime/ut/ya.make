UNITTEST_FOR(contrib/ydb/core/kqp/runtime)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:4)

SRCS(
    kqp_scan_data_ut.cpp
    kqp_scan_fetcher_ut.cpp
    scheduler/kqp_compute_scheduler_service_ut.cpp
    scheduler/kqp_compute_scheduler_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/kqp/workload_service/ut/common
    contrib/ydb/core/testlib/basics/pg
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/providers/yt/codec/codegen
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm16
    contrib/ydb/library/yql/providers/yt/comp_nodes/dq/llvm16
)

END()
