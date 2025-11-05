LIBRARY()

SRCS(
    yt_url_lister.cpp
)

PEERDIR(
    library/cpp/cgiparam
    contrib/ydb/library/yql/core/url_lister/interface
    contrib/ydb/library/yql/providers/yt/lib/init_yt_api
    contrib/ydb/library/yql/utils/log
    yt/cpp/mapreduce/interface
)

END()
