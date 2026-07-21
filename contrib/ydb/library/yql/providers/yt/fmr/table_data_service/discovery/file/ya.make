LIBRARY()

SRCS(
    yql_yt_file_service_discovery.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/discovery/interface
    contrib/ydb/library/yql/utils
)

YQL_LAST_ABI_VERSION()

END()
