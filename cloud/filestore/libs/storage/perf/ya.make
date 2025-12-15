Y_BENCHMARK(cloud-filestore-libs-storage-perf)

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

PEERDIR(
    cloud/filestore/libs/storage/tablet

    contrib/ydb/library/yql/public/udf

    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

SRCS(
    compaction_map.cpp
)

END()
