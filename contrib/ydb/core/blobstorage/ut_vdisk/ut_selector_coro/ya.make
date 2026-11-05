UNITTEST()

FORK_SUBTESTS()
SIZE(MEDIUM)
REQUIREMENTS(cpu:4 ram:4)

SRCS(
    selector_equivalence_ut.cpp
)

PEERDIR(
    contrib/ydb/apps/version
    contrib/ydb/core/blobstorage/vdisk/hulldb
    contrib/ydb/core/blobstorage/vdisk/hulldb/test
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()

RECURSE_FOR_TESTS(bench)
