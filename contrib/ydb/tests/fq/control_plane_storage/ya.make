UNITTEST_FOR(contrib/ydb/core/fq/libs/control_plane_storage)

SIZE(MEDIUM)

SPLIT_FACTOR(18)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ENDIF()

PEERDIR(
    library/cpp/retry
    library/cpp/testing/unittest
    contrib/ydb/core/external_sources
    contrib/ydb/core/fq/libs/actors/logging
    contrib/ydb/core/fq/libs/init
    contrib/ydb/core/fq/libs/quota_manager/events
    contrib/ydb/core/fq/libs/rate_limiter/control_plane_service
    contrib/ydb/core/fq/libs/rate_limiter/events
    contrib/ydb/core/testlib/default
    contrib/ydb/library/security
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

YQL_LAST_ABI_VERSION()

SRCS(
    in_memory_control_plane_storage_ut.cpp
    ydb_control_plane_storage_bindings_permissions_ut.cpp
    ydb_control_plane_storage_bindings_ut.cpp
    ydb_control_plane_storage_connections_permissions_ut.cpp
    ydb_control_plane_storage_connections_ut.cpp
    ydb_control_plane_storage_internal_ut.cpp
    ydb_control_plane_storage_queries_permissions_ut.cpp
    ydb_control_plane_storage_queries_ut.cpp
    ydb_control_plane_storage_quotas_ut.cpp
    ydb_control_plane_storage_ut.cpp
)

END()
