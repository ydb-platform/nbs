LIBRARY()

SRCS(
    yql_yt_table_data_service_helpers.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/client/impl
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/discovery/file
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/local/impl
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/server
)

YQL_LAST_ABI_VERSION()

END()
