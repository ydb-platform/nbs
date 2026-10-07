LIBRARY()

#INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

GENERATE_ENUM_SERIALIZATION(operation_status.h)

SRCS(
    barrier.cpp
    blob_markers.cpp
    block_index.cpp
    checkpoint.cpp
    commit_queue.cpp
    fresh_blob.cpp
    group_downtimes.cpp
    operation_status.cpp
    resource_metrics_updates_queue.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/storage/protos
    cloud/blockstore/libs/storage/protos_ydb
    cloud/blockstore/public/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/tablet

    contrib/ydb/library/actors/protos

    contrib/ydb/core/protos

    library/cpp/protobuf/json
)

END()

RECURSE_FOR_TESTS(
    ut
)
