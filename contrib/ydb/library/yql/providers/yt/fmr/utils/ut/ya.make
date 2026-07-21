UNITTEST()

SRCS(
    yql_yt_util_helpers_ut.cpp
    yql_yt_parser_fragment_list_index_ut.cpp
    yql_yt_binary_yson_comparator_ut.cpp
    yql_yt_index_serialisation_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/test_tools/yson
    contrib/ydb/library/yql/providers/yt/fmr/test_utils
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/providers/yt/fmr/request_options
    contrib/ydb/library/yql/providers/yt/fmr/yt_job_service/file
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/arrow
    contrib/ydb/library/yql/minikql/dom
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/providers/yt/job
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/providers/yt/codec/codegen/llvm16
    contrib/ydb/library/yql/minikql/codegen/llvm16
    contrib/ydb/library/yql/minikql/computation/llvm16
)

YQL_LAST_ABI_VERSION()

END()
