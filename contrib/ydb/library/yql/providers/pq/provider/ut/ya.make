UNITTEST_FOR(contrib/ydb/library/yql/providers/pq/provider)

TAG(ya:manual)

SRCS(
    yql_pq_ut.cpp
)

PEERDIR(
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    contrib/ydb/library/actors/wilson/protos
    contrib/ydb/library/yql/core/facade
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/core/services/mounts
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/transform
    contrib/ydb/library/yql/minikql/comp_nodes/llvm14
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    contrib/ydb/library/yql/providers/dq/local_gateway
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/pq/async_io
    contrib/ydb/library/yql/providers/pq/gateway/dummy
    contrib/ydb/library/yql/providers/pq/provider
    contrib/ydb/library/yql/providers/solomon/gateway
    contrib/ydb/library/yql/providers/solomon/provider
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/public/sdk/cpp/client/ydb_params
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
)

YQL_LAST_ABI_VERSION()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
