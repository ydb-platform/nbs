IF (OS_LINUX)
YQL_UDF_TEST()
    DEPENDS(
        contrib/ydb/library/yql/udfs/common/digest
        contrib/ydb/library/yql/udfs/common/string
        contrib/ydb/library/yql/udfs/common/streaming
    )
    TIMEOUT(300)
    SIZE(MEDIUM)

    IF (SANITIZER_TYPE == "memory")
        TAG(ya:not_autocheck) # YQL-15385
    ENDIF()
    END()

ENDIF()
