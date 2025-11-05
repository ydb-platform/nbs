IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            libstat_udf.so
        )
    END()
ELSE ()
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
ENDIF ()


RECURSE_FOR_TESTS(
    ut
)

