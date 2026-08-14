UNITTEST_FOR(contrib/ydb/library/yql/providers/dq/global_worker_manager)

NO_BUILD_IF(OS_WINDOWS)

SIZE(SMALL)

PEERDIR(
    contrib/ydb/library/actors/testlib
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    contrib/ydb/library/yql/providers/dq/actors/yt
    contrib/ydb/library/yql/providers/dq/actors
    contrib/ydb/library/yql/dq/actors/compute
    yql/essentials/minikql/computation/llvm16
    yql/essentials/minikql/comp_nodes/llvm16

    yql/essentials/core/dq_integration/transform
    contrib/ydb/library/yql/dq/comp_nodes
    yql/essentials/providers/common/comp_nodes
    yql/essentials/minikql/comp_nodes
    contrib/ydb/library/yql/dq/transform
    contrib/ydb/library/yql/providers/dq/task_runner    
)

SRCS(
    global_worker_manager_ut.cpp
    workers_storage_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/supp/ubsan_supp.inc)

YQL_LAST_ABI_VERSION()

END()
