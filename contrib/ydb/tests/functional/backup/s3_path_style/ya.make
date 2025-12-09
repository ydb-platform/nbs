UNITTEST()

ENV(S3_IGNORE_SUBDOMAIN_BUCKETNAME=true)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)

ENV(YDB_ERASURE=block_4-2)

PEERDIR(
    contrib/ydb/library/testlib/s3_recipe_helper
    contrib/ydb/public/sdk/cpp/src/client/export
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/operation
    contrib/ydb/public/sdk/cpp/src/client/draft
)

SRCS(
    s3_path_style_backup_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/s3_recipe/recipe.inc)

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16 cpu:4)
ENDIF()

END()
