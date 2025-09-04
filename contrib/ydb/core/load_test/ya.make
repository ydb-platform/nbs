LIBRARY()

CFLAGS(
    -Wno-deprecated-declarations
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/histogram/hdr
    library/cpp/monlib/dynamic_counters/percentile
    library/cpp/monlib/service/pages
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/backpressure
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/pdisk
    contrib/ydb/core/control
    contrib/ydb/core/keyvalue
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/rm_service
    contrib/ydb/core/tx/columnshard
    contrib/ydb/core/tx/datashard
    contrib/ydb/library/workload
    contrib/ydb/public/lib/base
    contrib/ydb/public/lib/operation_id
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/services/kesus
    contrib/ydb/services/metadata
    contrib/ydb/services/persqueue_cluster_discovery
    contrib/ydb/services/ydb
)

SRCS(
    aggregated_result.cpp
    archive.cpp
    config_examples.cpp
    keyvalue_write.cpp
    kqp.cpp
    memory.cpp
    pdisk_log.cpp
    pdisk_read.cpp
    pdisk_write.cpp
    service_actor.cpp
    group_write.cpp
    vdisk_write.cpp
    yql_single_query.cpp

    ycsb/actors.h
    ycsb/bulk_mkql_upsert.cpp
    ycsb/common.h
    ycsb/common.cpp
    ycsb/defs.h
    ycsb/info_collector.h
    ycsb/info_collector.cpp
    ycsb/kqp_select.cpp
    ycsb/kqp_upsert.cpp
    ycsb/test_load_actor.cpp
    ycsb/test_load_actor.h
    ycsb/test_load_read_iterator.cpp
)

GENERATE_ENUM_SERIALIZATION(percentile.h)

END()

RECURSE_FOR_TESTS(
    ut_ycsb
)
