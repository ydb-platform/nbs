UNITTEST()

SRCS(
    array_builder_ut.cpp
    bit_util_ut.cpp
    block_array_tree_ut.cpp
    block_reader_ut.cpp
    dense_union_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/yql/public/udf/arrow
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm16
    contrib/ydb/library/yql/utils
)

YQL_LAST_ABI_VERSION()

END()
