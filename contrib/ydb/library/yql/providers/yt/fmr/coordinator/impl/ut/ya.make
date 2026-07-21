UNITTEST()

SRCS(
    yql_yt_coordinator_ut.cpp
    yql_yt_gateway_coordinator_integration_ut.cpp
    yql_yt_ordered_partitioner_ut.cpp
    yql_yt_fmr_partitioner_ut.cpp
    yql_yt_sorted_partitioner_ut.cpp
    yql_yt_sorted_partitioner_base_ut.cpp
    yql_yt_reduce_partitioner_ut.cpp
    yql_yt_fmr_boundary_keys_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/impl
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file
    contrib/ydb/library/yql/providers/yt/fmr/job/impl
    contrib/ydb/library/yql/providers/yt/fmr/job_factory/impl
    contrib/ydb/library/yql/providers/yt/fmr/job_preparer/impl
    contrib/ydb/library/yql/providers/yt/fmr/worker/impl
    contrib/ydb/library/yql/providers/yt/fmr/test_tools/fmr_coordinator_service_helper
    contrib/ydb/library/yql/providers/yt/fmr/test_tools/fmr_gateway_helpers
    contrib/ydb/library/yql/providers/yt/fmr/test_tools/mock_time_provider
    contrib/ydb/library/yql/providers/yt/fmr/test_tools/sorted_partitioner
    contrib/ydb/library/yql/providers/yt/fmr/test_tools/table_data_service
    contrib/ydb/library/yql/providers/yt/fmr/utils/yson_block_iterator/impl
    contrib/ydb/library/yql/providers/yt/gateway/fmr
    contrib/ydb/library/yql/providers/yt/gateway/file
    contrib/ydb/library/yql/providers/yt/codec/codegen/llvm16
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm16
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm16
    contrib/ydb/library/yql/core/file_storage/proto
    contrib/ydb/library/yql/public/udf/service/terminate_policy
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/utils/failure_injector
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
