LIBRARY()

#INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

GENERATE_ENUM_SERIALIZATION(operation_status.h)

SRCS(
    barrier.cpp
    blob_markers.cpp
    block.cpp
    block_index.cpp
    checkpoint.cpp
    commit_queue.cpp
    fresh_blob.cpp
    group_downtimes.cpp
    operation_status.cpp
    part_counters_wrapper.cpp
    resource_metrics_updates_queue.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/public/api/protos
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/protos
    cloud/blockstore/libs/storage/protos_ydb

    cloud/storage/core/libs/common
    cloud/storage/core/libs/tablet

    library/cpp/protobuf/json

    contrib/ydb/library/actors/protos

    contrib/ydb/core/protos
    contrib/ydb/core/tablet
)

END()
