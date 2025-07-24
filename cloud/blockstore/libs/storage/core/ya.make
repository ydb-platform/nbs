LIBRARY()

GENERATE_ENUM_SERIALIZATION(mount_token.h)

SRCS(
    block_handler.cpp
    compaction_map.cpp
    compaction_options.cpp
    compaction_policy.cpp
    compaction_type.cpp
    config.cpp
    disk_counters.cpp
    disk_validation.cpp
    forward_helpers.cpp
    manually_preempted_volumes.cpp
    metrics.cpp
    monitoring_utils.cpp
    mount_token.cpp
    pending_request.cpp
    probes.cpp
    proto_helpers.cpp
    request_buffer.cpp
    request_info.cpp
    storage_request_counters.cpp
    tablet.cpp
    tablet_counters.cpp
    tablet_schema.cpp
    tenant.cpp
    ts_ring_buffer.cpp
    unimplemented.cpp
    volume_label.cpp
    volume_model.cpp
    write_buffer_request.cpp
    transaction_time_tracker.cpp
    group_request_tracker.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/service
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/model
    cloud/blockstore/libs/storage/protos
    cloud/blockstore/public/api/protos

    cloud/storage/core/config
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/features
    cloud/storage/core/libs/kikimr

    contrib/libs/openssl

    contrib/ydb/library/actors/core
    library/cpp/cgiparam
    library/cpp/containers/intrusive_rb_tree
    library/cpp/deprecated/atomic
    library/cpp/logger
    library/cpp/lwtrace
    library/cpp/monlib/service/pages
    library/cpp/openssl/crypto
    library/cpp/protobuf/util
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote

    contrib/ydb/core/base
    contrib/ydb/core/control
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/mon
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/library/yql/sql/pg_dummy
)

END()

RECURSE_FOR_TESTS(
    ut
    benchmark
)
