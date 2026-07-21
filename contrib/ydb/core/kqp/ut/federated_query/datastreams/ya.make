UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(200)

REQUIREMENTS(cpu:2)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    common.cpp
    datastreams_ut.cpp
    datastreams_table_mode_ut.cpp
    kqp_has_path_ut.cpp
    streaming_ddl_ut.cpp
    streaming_sys_view_ut.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    library/cpp/threading/local_executor
    contrib/ydb/core/cms/console
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/kqp/ut/federated_query/common
    contrib/ydb/core/sys_view/common
    contrib/ydb/core/testlib
    contrib/ydb/library/testlib/common
    contrib/ydb/library/testlib/pq_helpers
    contrib/ydb/library/testlib/s3_recipe_helper
    contrib/ydb/library/testlib/solomon_helpers
    contrib/ydb/library/yql/providers/generic/connector/libcpp
    contrib/ydb/library/yql/providers/generic/connector/libcpp/ut_helpers
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/udfs/common/yson2
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/s3_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)

YQL_LAST_ABI_VERSION()

END()
