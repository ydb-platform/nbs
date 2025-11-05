LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/discovery
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/replication/common
    contrib/ydb/core/tx/replication/ydb_proxy
    contrib/ydb/core/tx/scheme_board
    contrib/ydb/core/util
    contrib/ydb/core/ydb_convert
    contrib/ydb/services/metadata
    library/cpp/json
)

SRCS(
    controller.cpp
    dst_alterer.cpp
    dst_creator.cpp
    dst_remover.cpp
    lag_provider.cpp
    logging.cpp
    nodes_manager.cpp
    private_events.cpp
    replication.cpp
    secret_resolver.cpp
    session_info.cpp
    stream_creator.cpp
    stream_remover.cpp
    sys_params.cpp
    target_base.cpp
    target_discoverer.cpp
    target_table.cpp
    target_with_stream.cpp
    tenant_resolver.cpp
    tx_alter_dst_result.cpp
    tx_alter_replication.cpp
    tx_assign_stream_name.cpp
    tx_create_dst_result.cpp
    tx_create_replication.cpp
    tx_create_stream_result.cpp
    tx_describe_replication.cpp
    tx_discovery_targets_result.cpp
    tx_drop_dst_result.cpp
    tx_drop_replication.cpp
    tx_drop_stream_result.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_resolve_secret_result.cpp
    tx_worker_error.cpp
)

GENERATE_ENUM_SERIALIZATION(replication.h)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut_dst_creator
    ut_target_discoverer
)
