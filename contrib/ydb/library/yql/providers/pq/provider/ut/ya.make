UNITTEST_FOR(contrib/ydb/library/yql/providers/pq/provider)

SRCS(
    yql_pq_ut.cpp
)

PEERDIR(
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    contrib/ydb/library/actors/wilson/protos
    yql/essentials/core/facade
    yql/essentials/core/file_storage
    yql/essentials/core/services/mounts
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/transform
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    contrib/ydb/library/yql/providers/dq/local_gateway
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/pq/async_io
    contrib/ydb/library/yql/providers/pq/gateway/dummy
    contrib/ydb/library/yql/providers/pq/provider
    contrib/ydb/library/yql/providers/solomon/gateway
    contrib/ydb/library/yql/providers/solomon/provider
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    contrib/ydb/public/sdk/cpp/src/client/params
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/codecs
)

YQL_LAST_ABI_VERSION()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
