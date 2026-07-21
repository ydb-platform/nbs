UNITTEST()

SRCS(
    yql_yt_job_ut.cpp
    yql_yt_map_job_ut.cpp
    yql_yt_raw_table_queue_ut.cpp
    yql_yt_table_data_service_reader_ut.cpp
    yql_yt_table_data_service_writer_ut.cpp
    yql_yt_sorted_merge_reader_ut.cpp
    yql_yt_sorting_reader_ut.cpp
    yql_yt_table_queue_writer_with_lock_ut.cpp
)

PEERDIR(
    yt/cpp/mapreduce/common
    contrib/ydb/library/yql/providers/yt/fmr/job/impl
    contrib/ydb/library/yql/providers/yt/fmr/process
    contrib/ydb/library/yql/providers/yt/fmr/request_options/proto_helpers
    contrib/ydb/library/yql/providers/yt/fmr/test_tools/fmr_block_iterator
    contrib/ydb/library/yql/providers/yt/fmr/test_tools/table_data_service
    contrib/ydb/library/yql/providers/yt/fmr/test_tools/yson
    contrib/ydb/library/yql/providers/yt/fmr/test_utils
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/providers/yt/fmr/utils/yson_block_iterator/impl
    contrib/ydb/library/yql/utils/log
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
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm16
    contrib/ydb/library/yql/providers/yt/gateway/file
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/local/impl
)

YQL_LAST_ABI_VERSION()

END()
