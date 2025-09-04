UNITTEST_FOR(contrib/ydb/core/blobstorage)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

SIZE(MEDIUM)

SRCS(
    defaults.h
    gen_restarts.cpp
    gen_restarts.h
    huge_migration_ut.cpp
    mon_reregister_ut.cpp
    vdisk_test.cpp
)

PEERDIR(
    contrib/ydb/apps/version
    contrib/ydb/library/actors/protos
    library/cpp/codecs
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/blobstorage/ut_vdisk/lib
    contrib/ydb/core/erasure
    contrib/ydb/core/scheme
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
