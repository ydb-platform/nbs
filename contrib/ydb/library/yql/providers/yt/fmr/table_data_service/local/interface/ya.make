LIBRARY()

SRCS(
    yql_yt_table_data_service_local_interface.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/interface
)

YQL_LAST_ABI_VERSION()

END()
