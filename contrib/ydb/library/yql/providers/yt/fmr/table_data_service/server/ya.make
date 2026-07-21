LIBRARY()

SRCS(
    yql_yt_table_data_service_server.cpp
)

PEERDIR(
    library/cpp/http/server
    library/cpp/yson/node
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/local/interface
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/client/proto_helpers
    contrib/ydb/library/yql/providers/yt/fmr/tvm/interface
    contrib/ydb/library/yql/providers/yt/fmr/utils
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
