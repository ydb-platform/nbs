YQL_UDF_TEST()

DEPENDS(contrib/ydb/library/yql/udfs/common/compress_base)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

SIZE(MEDIUM)

END()
