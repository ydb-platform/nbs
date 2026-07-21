YQL_UDF_CONTRIB(yson2_udf)
    
    YQL_ABI_VERSION(
        2
        46
        0
    )

    SRCS(
        yson2_udf.cpp
    )
    
    PEERDIR(
        library/cpp/containers/stack_vector
        library/cpp/yson_pull
        contrib/ydb/library/yql/minikql/dom
        contrib/ydb/library/yql/public/langver
    )
    
    END()

RECURSE_FOR_TESTS(
    test
)

