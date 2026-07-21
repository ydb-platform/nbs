YQL_UDF_CONTRIB(reservoir_sampling_udf)
    
    YQL_ABI_VERSION(
        2
        43
        0
    )

    SRCS(
        reservoir_udf.cpp
    )

PEERDIR(
    contrib/ydb/library/yql/udfs/common/reservoir_sampling/lib
)

END()

RECURSE_FOR_TESTS(test)
