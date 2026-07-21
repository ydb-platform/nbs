UNITTEST_FOR(contrib/ydb/library/yql/core/spilling)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE OR NOT OPENSOURCE)
    REQUIREMENTS(ram:10)
ENDIF()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

# https://github.com/ydb-platform/ydb/issues/12513
IF (SANITIZER_TYPE != "address")
    SRCS(
        spilling_ut.cpp
    )
ENDIF()

PEERDIR(
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

END()
