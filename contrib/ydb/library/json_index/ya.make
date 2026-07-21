LIBRARY()

SRCS(
    json_index.cpp
)

PEERDIR(
    library/cpp/json
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/minikql/jsonpath/parser
    contrib/ydb/library/yql/types/binary_json
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
