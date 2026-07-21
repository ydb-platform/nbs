UNITTEST_FOR(contrib/ydb/core/kqp/proxy_service)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    kqp_proxy_ut.cpp
    kqp_script_executions_ut.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    contrib/ydb/core/kqp/run_script_actor
    contrib/ydb/core/kqp/proxy_service
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/kqp/workload_service/ut/common
    contrib/ydb/public/lib/ut_helpers
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/query
    contrib/ydb/services/ydb
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
