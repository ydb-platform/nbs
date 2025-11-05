LIBRARY()

SRCS(
    yt_download.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/lib/init_yt_api
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/utils
    library/cpp/cgiparam
    library/cpp/digest/md5
)

END()
