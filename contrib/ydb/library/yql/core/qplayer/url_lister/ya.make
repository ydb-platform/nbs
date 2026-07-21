LIBRARY()

SRCS(
    qplayer_url_lister_manager.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/qplayer/storage/interface
    contrib/ydb/library/yql/core/url_lister/interface
    contrib/ydb/library/yql/core
    library/cpp/yson/node
    contrib/libs/openssl
)

END()

