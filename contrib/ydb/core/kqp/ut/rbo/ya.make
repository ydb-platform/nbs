UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    kqp_rbo_yql_ut.cpp
    kqp_rbo_olap_ut.cpp
)

PEERDIR(
    library/cpp/resource
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/benchmarks/queries/tpch
    contrib/ydb/public/lib/ut_helpers
    contrib/ydb/library/yql/udfs/statistics_internal
    contrib/ydb/core/kqp/ut/olap/helpers
    contrib/ydb/core/statistics/ut_common
    contrib/ydb/library/yql/udfs/common/digest
    contrib/ydb/library/yql/udfs/common/hyperloglog
)

ADDINCL(
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/include
)

DATA (
    arcadia/contrib/ydb/core/kqp/ut/join/data
    arcadia/contrib/ydb/core/kqp/ut/rbo/data
)

RESOURCE(
    contrib/ydb/library/benchmarks/gen_queries/consts.yql consts.yql
    contrib/ydb/library/benchmarks/gen_queries/consts_decimal.yql consts_decimal.yql
)


IF (OS_WINDOWS)
CFLAGS(
   "-D__thread=__declspec(thread)"
   -Dfstat=microsoft_native_fstat
   -Dstat=microsoft_native_stat
)
ENDIF()

NO_COMPILER_WARNINGS()

YQL_LAST_ABI_VERSION()

END()
