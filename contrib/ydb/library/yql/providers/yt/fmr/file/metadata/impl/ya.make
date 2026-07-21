LIBRARY()

SRCS(
    yql_yt_file_metadata_impl.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/file/metadata/interface
    contrib/ydb/library/yql/providers/yt/fmr/request_options
    contrib/ydb/library/yql/providers/yt/fmr/utils
)

YQL_LAST_ABI_VERSION()

END()
