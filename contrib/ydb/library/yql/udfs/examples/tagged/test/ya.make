YQL_UDF_TEST_CONTRIB()

TIMEOUT(300)
SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/library/yql/udfs/examples/tagged
)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

END()

