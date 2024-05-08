Y_BENCHMARK(cloud-filestore-libs-storage-perf)

PEERDIR(
    cloud/filestore/libs/storage/tablet

    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

SRCS(
    compaction_map.cpp
)

END()
