YQL_UDF_CONTRIB(protobuf_udf)

YQL_ABI_VERSION(
    2
    9
    0
)

SRCS(
    protobuf_udf.cpp
)

PEERDIR(
    library/cpp/protobuf/yql
    contrib/ydb/library/yql/minikql/protobuf_udf
    contrib/ydb/library/yql/public/udf
)

END()

RECURSE_FOR_TESTS(
    test
)
