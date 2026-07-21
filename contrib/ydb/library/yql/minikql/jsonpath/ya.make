LIBRARY()

YQL_ABI_VERSION(
    2
    27
    0
)

IF (ARCH_X86_64)
    PEERDIR(
        contrib/ydb/library/yql/minikql/jsonpath/rewrapper/hyperscan
    )
ENDIF()

PEERDIR(
    library/cpp/json
    contrib/ydb/library/yql/minikql/jsonpath/rewrapper/re2
    contrib/ydb/library/yql/minikql/jsonpath/rewrapper
    contrib/ydb/library/yql/minikql/jsonpath/parser
    contrib/ydb/library/yql/types/binary_json
    contrib/ydb/library/yql/minikql/dom
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/public/issue/protos
)

SRCS(
    executor.cpp
    jsonpath.cpp
    value.cpp
)

END()

RECURSE(
    benchmark
    parser
    rewrapper
)

RECURSE_FOR_TESTS(
    ut
)
