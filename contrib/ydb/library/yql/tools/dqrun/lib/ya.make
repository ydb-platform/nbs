LIBRARY()

SRCS(
    dqrun_lib.cpp
)

PEERDIR(
    yt/yql/tools/ytrun/lib
    contrib/ydb/library/yql/providers/yt/gateway/file
    contrib/ydb/library/yql/providers/yt/gateway/native
    contrib/ydb/library/yql/providers/yt/provider
    contrib/ydb/library/yql/providers/yt/gateway/file
    contrib/ydb/library/yql/providers/yt/comp_nodes/dq
    contrib/ydb/library/yql/providers/yt/mkql_dq

    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/common/metrics
    contrib/ydb/library/yql/core/dq_integration
    contrib/ydb/library/yql/core/dq_integration/transform
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/tools/yql_facade_run
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/utils/log

    contrib/ydb/core/fq/libs/db_id_async_resolver_impl
    contrib/ydb/core/fq/libs/shared_resources/interface
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/core/fq/libs/init
    contrib/ydb/core/fq/libs/actors

    contrib/ydb/library/yql/providers/clickhouse/provider
    contrib/ydb/library/yql/providers/clickhouse/actors

    contrib/ydb/library/yql/providers/ydb/provider
    contrib/ydb/library/yql/providers/ydb/comp_nodes
    contrib/ydb/library/yql/providers/ydb/actors

    contrib/ydb/library/yql/providers/s3/provider
    contrib/ydb/library/yql/providers/s3/actors

    contrib/ydb/library/yql/providers/pq/provider
    contrib/ydb/library/yql/providers/pq/gateway/abstract
    contrib/ydb/library/yql/providers/pq/async_io

    contrib/ydb/library/yql/providers/solomon/provider
    contrib/ydb/library/yql/providers/solomon/gateway
    contrib/ydb/library/yql/providers/solomon/actors

    contrib/ydb/library/yql/providers/generic/connector/libcpp
    contrib/ydb/library/yql/providers/generic/provider
    contrib/ydb/library/yql/providers/generic/actors

    contrib/ydb/library/yql/providers/dq/interface
    contrib/ydb/library/yql/providers/dq/local_gateway
    contrib/ydb/library/yql/providers/dq/provider/exec
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/dq/helper

    contrib/ydb/library/yql/providers/yt/dq_task_preprocessor
    contrib/ydb/library/yql/providers/yt/actors

    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/providers/common/db_id_async_resolver

    contrib/ydb/library/yql/dq/opt
    contrib/ydb/library/yql/dq/transform
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/actors/input_transforms

    contrib/ydb/library/yql/utils/bindings
    contrib/ydb/library/yql/utils/actor_system

    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/http
)

YQL_LAST_ABI_VERSION()

SUPPRESSIONS(
    lsan.supp
)

END()
