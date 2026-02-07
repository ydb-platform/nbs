LIBRARY()

SRCS(
    yql_yt_qplayer_gateway.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/qplayer/storage/interface
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/file_storage
    library/cpp/yson/node
    library/cpp/random_provider
    yt/cpp/mapreduce/interface
    contrib/libs/openssl
)

YQL_LAST_ABI_VERSION()

END()


