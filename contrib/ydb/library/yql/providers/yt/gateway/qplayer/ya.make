LIBRARY()

SRCS(
    yql_yt_qplayer_gateway.cpp
)

PEERDIR(
    contrib/libs/openssl
    library/cpp/yson/node
    library/cpp/random_provider
    yt/cpp/mapreduce/interface
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/core/services
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/providers/yt/expr_nodes
    contrib/ydb/library/yql/providers/yt/lib/dump_helpers
    contrib/ydb/library/yql/providers/yt/lib/full_capture
)

YQL_LAST_ABI_VERSION()

END()
