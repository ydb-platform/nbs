UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_document_api_ut.cpp
    kqp_qs_queries_ut.cpp
    kqp_qs_scripts_ut.cpp
    kqp_service_ut.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/threading/local_executor
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/tx/columnshard/hooks/testing
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    contrib/ydb/public/lib/ut_helpers
    contrib/ydb/public/sdk/cpp/src/client/operation
    contrib/ydb/public/sdk/cpp/src/client/types/operation
)

YQL_LAST_ABI_VERSION()

END()
