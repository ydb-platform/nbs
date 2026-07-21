YQL_UDF_CONTRIB(stat_udf)
    
    YQL_ABI_VERSION(
        2
        28
        0
    )

    SRCS(
        stat_udf.cpp
    )
    
    PEERDIR(
        contrib/ydb/library/yql/udfs/common/stat/static
    )
    
    END()

IF (NOT EXPORT_CMAKE)
    RECURSE_FOR_TESTS(
        test
        ut
    )
ENDIF()
