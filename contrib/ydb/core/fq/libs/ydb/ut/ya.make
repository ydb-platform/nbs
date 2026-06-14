UNITTEST_FOR(contrib/ydb/core/fq/libs/ydb)

FORK_SUBTESTS()

SRCS(
    ydb_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/library/security
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

END()
