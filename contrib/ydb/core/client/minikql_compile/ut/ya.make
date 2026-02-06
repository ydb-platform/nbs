UNITTEST_FOR(contrib/ydb/core/client/minikql_compile)

ALLOCATOR(J)

SRCS(
    yql_expr_minikql_compile_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/threading/future
    contrib/ydb/core/client/scheme_cache_lib
    contrib/ydb/core/client/server
    contrib/ydb/core/tablet
    contrib/ydb/core/testlib/default
    yql/essentials/minikql
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
