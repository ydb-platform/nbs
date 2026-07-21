LIBRARY()

SRCS(
    yql_yt_table_data_service_local.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/local/interface
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/utils/log
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
