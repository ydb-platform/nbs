Y_BENCHMARK(cloud-filestore-libs-storage-perf)

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

PEERDIR(
    cloud/filestore/libs/storage/tablet

    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

SRCS(
    compaction_map.cpp
)

END()
