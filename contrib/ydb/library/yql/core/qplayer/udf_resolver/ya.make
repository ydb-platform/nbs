LIBRARY()

SRCS(
    yql_qplayer_udf_resolver.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/qplayer/storage/interface
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/core
    library/cpp/yson/node
    contrib/libs/openssl
)

END()

