FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SET(ORIG_SRC_DIR contrib/ydb/library/yql/providers/yt/comp_nodes/ut)

SET(ORIG_SOURCES
    yql_mkql_output_ut.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/time_provider
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/providers/yt/codec
    contrib/ydb/library/yql/providers/yt/codec/codegen
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

COPY(
    WITH_CONTEXT
    AUTO
    FROM ${ORIG_SRC_DIR}
    ${ORIG_SOURCES}
)
