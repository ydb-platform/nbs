LIBRARY()

SRCS(
    aggregator.h
    aggregator.cpp
    aggregator_impl.h
    aggregator_impl.cpp
    schema.h
    schema.cpp
    tx_configure.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_schemeshard_stats.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
)

YQL_LAST_ABI_VERSION()

END()
