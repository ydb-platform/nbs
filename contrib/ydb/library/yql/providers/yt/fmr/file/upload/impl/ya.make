LIBRARY()

SRCS(
    yql_yt_file_upload_impl.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/file/upload/interface
    contrib/ydb/library/yql/providers/yt/fmr/utils
    contrib/ydb/library/yql/providers/yt/fmr/request_options
    contrib/ydb/library/yql/providers/yt/gateway/lib

)

YQL_LAST_ABI_VERSION()

END()
