LIBRARY()

SRCS(
    yql_yt_fmr_gateway_helpers.cpp
)

PEERDIR(
    library/cpp/time_provider
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/invoke_builtins
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/interface
    contrib/ydb/library/yql/providers/yt/fmr/fmr_tool_lib
    contrib/ydb/library/yql/providers/yt/fmr/job_launcher
    contrib/ydb/library/yql/providers/yt/fmr/test_tools/mock_time_provider
    contrib/ydb/library/yql/providers/yt/fmr/test_tools/table_data_service
    contrib/ydb/library/yql/providers/yt/fmr/yt_job_service/file
    contrib/ydb/library/yql/providers/yt/gateway/file
    contrib/ydb/library/yql/providers/yt/gateway/fmr
)

YQL_LAST_ABI_VERSION()

END()

