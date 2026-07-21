
SET(ORIG_SRC_DIR contrib/ydb/library/yql/providers/yt/codec/codegen/ut)

SET(ORIG_SOURCES
    yt_codec_cg_ut.cpp
)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
ENDIF()

ADDINCL(
    contrib/ydb/library/yql/providers/yt/codec/codegen
)

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/providers/yt/codec
)

YQL_LAST_ABI_VERSION()

IF (MKQL_DISABLE_CODEGEN)
    CFLAGS(
        -DMKQL_DISABLE_CODEGEN
    )
ENDIF()

COPY(
    WITH_CONTEXT
    AUTO
    FROM ${ORIG_SRC_DIR}
    ${ORIG_SOURCES}
    OUTPUT_INCLUDES
        contrib/ydb/library/yql/minikql/computation/mkql_computation_node_holders.h
        llvm/IR/Module.h
)
