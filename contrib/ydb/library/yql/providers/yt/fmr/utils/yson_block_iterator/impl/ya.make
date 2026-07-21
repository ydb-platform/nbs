LIBRARY()

SRCS(
    yql_yt_yson_tds_block_iterator.cpp
    yql_yt_yson_yt_block_iterator.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yt/yson
    library/cpp/threading/future
    contrib/ydb/library/yql/utils
    yt/cpp/mapreduce/interface
    contrib/ydb/library/yql/providers/yt/fmr/request_options
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/interface
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/providers/yt/fmr/utils/hasher
    contrib/ydb/library/yql/providers/yt/fmr/utils/yson_block_iterator/interface
)

YQL_LAST_ABI_VERSION()

END()
