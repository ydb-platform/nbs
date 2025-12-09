YQL_UDF_TEST()

SIZE(MEDIUM)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

DEPENDS(contrib/ydb/library/yql/udfs/common/knn)

END()
