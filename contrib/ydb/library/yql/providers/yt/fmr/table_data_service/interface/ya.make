LIBRARY()

SRCS(
    yql_yt_table_data_service.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/library/yql/utils
)

YQL_LAST_ABI_VERSION()

END()
