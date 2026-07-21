YQL_UDF_CONTRIB(datetime2_udf)
    YQL_ABI_VERSION(
        2
        47
        0
    )

    SRCS(
        datetime_udf.cpp
    )
    PEERDIR(
        util/draft
        library/cpp/type_info/tz
        contrib/ydb/library/yql/public/udf/arrow
        contrib/ydb/library/yql/public/langver
        contrib/ydb/library/yql/minikql
        contrib/ydb/library/yql/minikql/datetime
    )
    END()

RECURSE_FOR_TESTS(
    test
    test_bigdates
)
