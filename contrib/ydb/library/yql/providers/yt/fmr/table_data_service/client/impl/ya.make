LIBRARY()

SRCS(
    yql_yt_table_data_service_client_impl.cpp
)

PEERDIR(
    library/cpp/threading/future
    library/cpp/http/simple
    library/cpp/retry
    library/cpp/yson/node
    contrib/ydb/library/yql/providers/yt/fmr/request_options
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/client/proto_helpers
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/discovery/file
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/interface
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
)

YQL_LAST_ABI_VERSION()

END()
