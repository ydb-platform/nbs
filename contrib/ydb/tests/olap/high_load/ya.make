UNITTEST_FOR(contrib/ydb/services/ydb)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

SRCS(
    read_update_write.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/pg
    contrib/ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

END()