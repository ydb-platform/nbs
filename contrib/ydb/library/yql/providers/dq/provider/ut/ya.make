UNITTEST_FOR(contrib/ydb/library/yql/providers/dq/provider)

SRCS(
    yql_dq_provider_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/transform
    contrib/ydb/library/yql/providers/dq/local_gateway
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/dq/provider/exec
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    library/cpp/testing/unittest
    contrib/ydb/library/yql/providers/yt/codec/codegen
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm16
    contrib/ydb/library/yql/providers/yt/gateway/file
    contrib/ydb/library/yql/providers/yt/lib/ut_common
    contrib/ydb/library/yql/providers/yt/provider
    contrib/ydb/library/yql/core/cbo/simple
    contrib/ydb/library/yql/core/facade
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/core/services/mounts
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg
)

YQL_LAST_ABI_VERSION()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
