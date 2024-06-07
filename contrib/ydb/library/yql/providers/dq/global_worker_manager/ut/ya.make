UNITTEST_FOR(contrib/ydb/library/yql/providers/dq/global_worker_manager)

NO_BUILD_IF(OS_WINDOWS)

SIZE(SMALL)

PEERDIR(
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/providers/dq/actors/yt
    contrib/ydb/library/yql/providers/dq/actors
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/minikql/computation/llvm14
    contrib/ydb/library/yql/minikql/comp_nodes/llvm14
)

SRCS(
    global_worker_manager_ut.cpp
    workers_storage_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/supp/ubsan_supp.inc)

YQL_LAST_ABI_VERSION()

END()
