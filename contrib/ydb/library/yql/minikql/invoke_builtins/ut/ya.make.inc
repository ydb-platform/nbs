FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

OWNER(g:yql)

PEERDIR(
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SET(ORIG_SRC_DIR contrib/ydb/library/yql/minikql/invoke_builtins)

SET(ORIG_SOURCES
    mkql_builtins_ut.cpp
)

ADDINCL(
    ${ORIG_SRC_DIR}
)

COPY(
    AUTO
    FROM ${ORIG_SRC_DIR}
    ${ORIG_SOURCES}
    OUTPUT_INCLUDES
        mkql_builtins.h
)
