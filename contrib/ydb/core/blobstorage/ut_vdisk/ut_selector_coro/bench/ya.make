PROGRAM(selector_coro_bench)

SRCS(main.cpp)

PEERDIR(
    contrib/ydb/apps/version
    contrib/ydb/core/blobstorage/vdisk/hulldb
    contrib/ydb/core/blobstorage/vdisk/hulldb/test
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()
