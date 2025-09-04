UNITTEST_FOR(contrib/ydb/core/fq/libs/checkpoint_storage)

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    library/cpp/retry
    library/cpp/testing/unittest
    contrib/ydb/core/fq/libs/actors/logging
    contrib/ydb/core/fq/libs/checkpoint_storage/events
    contrib/ydb/core/testlib/default
    contrib/ydb/library/security
    contrib/ydb/public/sdk/cpp/src/client/table
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

YQL_LAST_ABI_VERSION()

SRCS(
    gc_ut.cpp
    storage_service_ydb_ut.cpp
    ydb_state_storage_ut.cpp
    ydb_checkpoint_storage_ut.cpp
)

END()
