YQL_UDF_TEST()

DEPENDS(contrib/ydb/library/yql/udfs/common/roaring)

SIZE(MEDIUM)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

END()
