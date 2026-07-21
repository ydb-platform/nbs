YQL_UDF_CONTRIB(json2_udf)
    
    YQL_ABI_VERSION(
        2
        28
        0
    )

    SRCS(
        json2_udf.cpp
    )
    
    PEERDIR(
        contrib/ydb/library/yql/core/sql_types
        contrib/ydb/library/yql/types/binary_json
        contrib/ydb/library/yql/minikql/dom
        contrib/ydb/library/yql/minikql/jsonpath
    )
    
    END()

RECURSE_FOR_TESTS(
    test
)

