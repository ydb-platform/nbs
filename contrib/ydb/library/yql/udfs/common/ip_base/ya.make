IF (YQL_PACKAGED)
    PACKAGE()

    FROM_SANDBOX(
        FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            libip_udf.so
    )

    END()

ELSE()

YQL_UDF_CONTRIB(ip_udf)

    YQL_ABI_VERSION(
        2
        28
        0
    )

    SRCS(
        ip_base.cpp
    )

    PEERDIR(
        contrib/ydb/library/yql/udfs/common/ip_base/lib
    )

    END()

ENDIF()

RECURSE_FOR_TESTS(
    test
)