UNITTEST_FOR(contrib/ydb/library/yql/providers/solomon/async_io)

TAG(ya:manual)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)

SRCS(
    dq_solomon_write_actor_ut.cpp
    ut_helpers.cpp
)

PEERDIR(
    library/cpp/http/simple
    library/cpp/retry
    contrib/ydb/core/testlib/basics
    contrib/ydb/library/yql/minikql/computation/llvm14
    contrib/ydb/library/yql/minikql/comp_nodes/llvm14
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/common/ut_helpers
)

YQL_LAST_ABI_VERSION()

END()
