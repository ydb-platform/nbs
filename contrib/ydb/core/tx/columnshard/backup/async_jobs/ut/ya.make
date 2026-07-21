UNITTEST_FOR(contrib/ydb/core/tx/columnshard/backup/async_jobs)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/apps/ydbd/export
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/core/tx/columnshard/hooks/abstract
    contrib/ydb/core/tx/columnshard/hooks/testing
    contrib/ydb/core/tx/columnshard/test_helper
    contrib/ydb/library/aclib/protos
    contrib/ydb/public/lib/yson_value
    contrib/ydb/services/metadata
    contrib/ydb/library/testlib/s3_recipe_helper
)

YQL_LAST_ABI_VERSION()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/s3_recipe/recipe.inc)

SRCS(
    ut_import_downloader.cpp
)

END()
