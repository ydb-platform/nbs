LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/discovery
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/protos
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/replication/ydb_proxy
    contrib/ydb/core/util
    contrib/ydb/core/ydb_convert
)

SRCS(
    controller.cpp
    dst_creator.cpp
    dst_remover.cpp
    logging.cpp
    nodes_manager.cpp
    private_events.cpp
    replication.cpp
    stream_creator.cpp
    stream_remover.cpp
    sys_params.cpp
    target_base.cpp
    target_discoverer.cpp
    target_table.cpp
    target_with_stream.cpp
    tenant_resolver.cpp
    tx_assign_stream_name.cpp
    tx_create_dst_result.cpp
    tx_create_replication.cpp
    tx_create_stream_result.cpp
    tx_discovery_targets_result.cpp
    tx_drop_dst_result.cpp
    tx_drop_replication.cpp
    tx_drop_stream_result.cpp
    tx_init.cpp
    tx_init_schema.cpp
)

GENERATE_ENUM_SERIALIZATION(replication.h)

YQL_LAST_ABI_VERSION()

END()
