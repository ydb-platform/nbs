LIBRARY()

SRCS(
    yql_yt_static_service_discovery.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/discovery/interface
)

YQL_LAST_ABI_VERSION()

END()
