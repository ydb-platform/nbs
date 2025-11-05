UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_acl_ut.cpp
    kqp_scheme_ut.cpp
    kqp_constraints_ut.cpp
)

PEERDIR(
    library/cpp/threading/local_executor
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/kqp/workload_service/ut/common
    contrib/ydb/core/tx/columnshard/hooks/testing
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()
