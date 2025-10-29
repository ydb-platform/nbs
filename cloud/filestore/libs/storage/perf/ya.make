Y_BENCHMARK(cloud-filestore-libs-storage-perf)

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

PEERDIR(
    cloud/filestore/libs/storage/tablet

    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

SRCS(
    compaction_map.cpp
)

END()
